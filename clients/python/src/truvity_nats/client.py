"""The connection: verify-full, credentials read for every (re)connect, unlimited reconnect."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import ssl
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from nats import errors as nats_errors
from nats.aio.client import Client as _Driver
from nats.aio.client import Server
from nats.js import JetStreamContext, api

from .config import NatsConfig, redact_url
from .jetstream import (
    DEFAULT_PUBLISH_TIMEOUT,
    ConsumerSpec,
    StreamSpec,
    ensure_consumer,
    ensure_stream,
    publish,
)
from .retry import CredentialError, retry

HEALTH_TIMEOUT = 2.0
"""Bounds :meth:`NatsClient.health`, seconds."""

_log = logging.getLogger("truvity_nats")

# The adapter relies on these driver internals (see _Connection); fail loudly at import if a driver
# release moved one, instead of silently connecting without verification or reload.
for _hook in ("_process_info", "_process_err", "_process_op_err", "_attempt_reconnect", "_connect_command"):
    if not callable(getattr(_Driver, _hook, None)):
        raise ImportError(f"truvity_nats: unsupported nats-py version: {_hook} not found")
if not isinstance(getattr(_Driver, "ssl_context", None), property):
    raise ImportError("truvity_nats: unsupported nats-py version: ssl_context is not a property")


class NatsClientError(Exception):
    """An adapter error: connect failure, health, drain."""


class _Connection(_Driver):
    """
    nats-py's client with the adapter's three overrides.

    * ``ssl_context`` is nats-py's own public hook: it is read for every TLS handshake. The
      stock client takes one ``ssl.SSLContext`` at ``connect()`` and reuses it for every
      reconnect, so a renewed certificate would need a restart. Here a fresh context is built
      per handshake from the CA, certificate and key files (verify-full, trust only the CA file).
    * ``_process_info`` runs on the broker's first INFO, before anything is sent. With a CA file
      it refuses a broker that offers no TLS (nothing, token included, is sent in the clear) and
      starts the upgrade the broker only *offers* (``tls_available``, the tenancy preset's
      ``allow_non_tls``), which the stock client ignores.
    * ``_process_err`` turns the broker's authorization errors on an established connection
      (user JWT expired or revoked) into a reconnect; the stock client closes for good.
    """

    def __init__(self, cfg: NatsConfig) -> None:
        super().__init__()
        self._tv_cfg = cfg

    @property
    def ssl_context(self) -> ssl.SSLContext:
        return _build_ssl_context(self._tv_cfg)

    async def _process_info(self, info: dict[str, Any], initial_connection: bool = False) -> None:
        if initial_connection and self._tv_cfg.ca_file:
            if not (info.get("tls_required") or info.get("tls_available")):
                raise nats_errors.SecureConnWantedError()
            info["tls_required"] = True  # nats-py upgrades when it reads this, after this hook
        await super()._process_info(info, initial_connection)

    async def _process_err(self, err_msg: str) -> None:
        low = err_msg.lower()
        if (
            "authorization violation" in low
            or "authentication expired" in low
            or "authentication revoked" in low
        ):
            await self._process_op_err(nats_errors.AuthorizationError())
            return
        await super()._process_err(err_msg)


def _build_ssl_context(cfg: NatsConfig) -> ssl.SSLContext:
    """verify-full against the CA file, read now; the client certificate is loaded now too."""
    if not cfg.ca_file:
        # The broker requires TLS but no CA was given: nothing to verify against.
        raise nats_errors.SecureConnRequiredError()
    try:
        # With a cafile, create_default_context loads that file and not the system store.
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cfg.ca_file)
    except (OSError, ssl.SSLError) as err:
        raise CredentialError(f"natsclient: read CA file {cfg.ca_file}: {err}") from err
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    # Python 3.13 adds VERIFY_X509_STRICT, which refuses a CA certificate without a keyUsage extension
    # (as cert-manager's and the conformance broker's CAs may be). Chain, name and expiry checks stay;
    # Go and Node apply no such profile either.
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    if cfg.cert_file:
        try:
            ctx.load_cert_chain(cfg.cert_file, cfg.key_file)
        except (OSError, ssl.SSLError) as err:
            raise CredentialError(f"natsclient: load client certificate: {err}") from err
    return ctx


def _tls_hostname(url: str) -> str:
    return urlsplit(url).hostname or ""


class NatsClient:
    """
    One connection to the broker. It reconnects without limit; the credential
    files are read again for every reconnect. Create it with :meth:`connect`.
    """

    def __init__(self, config: NatsConfig, conn: _Connection) -> None:
        self.config = config
        self._nc = conn
        self._js: JetStreamContext | None = None

    @classmethod
    async def connect(cls, cfg: NatsConfig, *, logger: logging.Logger | None = None) -> NatsClient:
        """
        Validates ``cfg`` and opens the connection. The first connection is retried
        (see ``cfg.retry``) while the broker is not reachable; a failed verification or
        authorization is raised at once. Nothing logged contains a credential.
        """
        cfg.validate()
        log = logger or _log
        try:
            nc = await retry(cfg.retry, lambda: _open(cfg, log))
        except Exception as err:
            raise NatsClientError(f"natsclient: connect {redact_url(cfg.url)}: {err}") from err
        return cls(cfg, nc)

    @property
    def conn(self) -> _Driver:
        """The driver's connection, for what the adapter does not wrap (subscriptions, requests).
        Do not close it; use :meth:`close`."""
        return self._nc

    @property
    def js(self) -> JetStreamContext:
        """The driver's JetStream context."""
        if self._js is None:
            self._js = self._nc.jetstream()
        return self._js

    async def ensure_stream(self, spec: StreamSpec) -> api.StreamInfo:
        """Creates the stream or brings an existing one to the spec (see :class:`StreamSpec`)."""
        return await ensure_stream(self.js, spec)

    async def ensure_consumer(self, spec: ConsumerSpec) -> api.ConsumerInfo:
        """Creates the durable consumer or brings an existing one to the spec (see :class:`ConsumerSpec`)."""
        return await ensure_consumer(self.js, spec)

    async def publish(
        self,
        subject: str,
        data: bytes,
        *,
        msg_id: str = "",
        traceparent: str = "",
        tracestate: str = "",
        headers: Mapping[str, str] | None = None,
        timeout: float = DEFAULT_PUBLISH_TIMEOUT,
    ) -> api.PubAck:
        """Sends ``data`` to a JetStream subject and waits for the stream's acknowledgement."""
        return await publish(
            self.js,
            subject,
            data,
            msg_id=msg_id,
            traceparent=traceparent,
            tracestate=tracestate,
            headers=headers,
            timeout=timeout,
        )

    async def health(self) -> None:
        """
        A round trip to the broker through the real path (TLS, authentication), bounded to
        2s. Raises :class:`NatsClientError` when it is not connected or does not answer.
        """
        nc = self._nc
        if not nc.is_connected:
            state = "closed" if nc.is_closed else "draining" if nc.is_draining else "reconnecting"
            raise NatsClientError(f"natsclient: not connected ({state})")
        try:
            await asyncio.wait_for(nc.flush(timeout=int(HEALTH_TIMEOUT)), HEALTH_TIMEOUT)
        except (asyncio.TimeoutError, nats_errors.Error, OSError) as err:
            raise NatsClientError(f"natsclient: health check failed: {err or type(err).__name__}") from err

    async def close(self) -> None:
        """
        Drains the connection: subscriptions stop taking new messages, the ones already
        delivered are handled, pending publishes are flushed, then the connection closes.
        Returns when that is done; after ``drain_timeout`` the connection is closed and
        :class:`NatsClientError` is raised.
        """
        nc = self._nc
        if nc.is_closed:
            return
        try:
            # The driver bounds the drain by drain_timeout itself; the outer bound adds a margin.
            await asyncio.wait_for(nc.drain(), self.config.drain_timeout + 2.0)
        except asyncio.TimeoutError as err:
            await nc.close()
            raise NatsClientError(
                f"natsclient: drain did not finish in {self.config.drain_timeout + 2.0}s; connection closed"
            ) from err
        except nats_errors.ConnectionClosedError:
            return
        except nats_errors.Error as err:
            await nc.close()
            raise NatsClientError(f"natsclient: drain: {err}; connection closed") from err


def _read_token(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except (OSError, UnicodeDecodeError) as err:
        raise CredentialError(f"natsclient: token file {path} unreadable: {err}") from err


async def _open(cfg: NatsConfig, log: logging.Logger) -> _Connection:
    """One try at the first connection; the driver's own retry is bypassed (see README)."""
    nc = _Connection(cfg)
    seen: list[BaseException] = []

    async def on_error(err: Exception) -> None:
        seen.append(err)
        log.error("nats error: %s", err or type(err).__name__)

    async def on_disconnect() -> None:
        log.warning("nats disconnected")

    async def on_reconnect() -> None:
        log.info("nats reconnected")

    def reconnect_target(servers: list[Server], _info: dict[str, Any]) -> tuple[Server | None, float]:
        # The first try after a disconnect is immediate; later ones wait, with jitter.
        s = servers[0]
        delay = 0.0 if s.reconnects == 0 else cfg.reconnect_wait + random.uniform(0.0, cfg.reconnect_wait / 2)
        return s, delay

    # kwargs: the driver annotates its durations as int but uses them as float seconds.
    kwargs: dict[str, Any] = {
        "servers": [cfg.url],
        "name": cfg.name,
        "error_cb": on_error,
        "disconnected_cb": on_disconnect,
        "reconnected_cb": on_reconnect,
        "connect_timeout": cfg.connect_timeout,
        "ping_interval": cfg.ping_interval,
        "drain_timeout": cfg.drain_timeout,
        "dont_randomize": True,
        "reconnect_to_server_handler": reconnect_target,
        # The first connection must fail by name: the driver's connect() retries every error forever when
        # reconnect is allowed, and cycles through a refused address without limit when
        # max_reconnect_attempts is not positive. One extra try per adapter attempt, no wait between.
        "allow_reconnect": False,
        "max_reconnect_attempts": 1,
        "reconnect_time_wait": 0,
    }
    if cfg.ca_file:
        kwargs["tls_hostname"] = _tls_hostname(cfg.url)
    if cfg.token_file:
        token_file = cfg.token_file
        kwargs["token"] = lambda: _read_token(token_file)
    try:
        await nc.connect(**kwargs)
    except nats_errors.NoServersError as err:
        await _discard(nc)
        raise (seen[-1] if seen else err) from None
    except BaseException:
        await _discard(nc)
        raise
    if not nc.is_connected:
        await _quiet_close(nc)
        raise ConnectionError("natsclient: the broker did not complete the handshake")
    # From here on: reconnect without limit. The driver reads these at every disconnect; the pace and
    # jitter come from reconnect_target above.
    nc.options["allow_reconnect"] = True
    nc.options["max_reconnect_attempts"] = -1
    nc.options["reconnect_time_wait"] = cfg.reconnect_wait
    return nc


async def _discard(nc: _Driver) -> None:
    """Drops a connection whose handshake failed. The driver's own close() would wait for a flusher
    task that never started, so the socket is closed directly."""
    transport = getattr(nc, "_transport", None)
    if transport is not None:
        with contextlib.suppress(Exception):
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 1.0)


async def _quiet_close(nc: _Driver) -> None:
    with contextlib.suppress(Exception):  # best effort; the original error is what matters
        await nc.close()
