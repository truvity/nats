"""Configuration: the contract's inputs, validation and the environment reader."""

from __future__ import annotations

import math
import os
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import TypeVar
from urllib.parse import urlsplit

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Bounds the retry of the first connection: exponential backoff with full jitter."""

    attempts: int = 5
    """Total tries, including the first."""
    initial_delay: float = 0.2
    """Ceiling of the first sleep, seconds."""
    max_delay: float = 5.0
    """Ceiling of any sleep, seconds."""
    budget: float = 30.0
    """Total time that may be spent waiting, seconds."""

    def problem(self) -> str | None:
        if self.attempts >= 1 and 0 < self.initial_delay <= self.max_delay and self.budget > 0:
            return None
        return "retry: attempts>=1, 0<initial_delay<=max_delay, budget>0"


class ConfigError(ValueError):
    """Every problem with a configuration, at once."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("natsclient: invalid configuration:\n  - " + "\n  - ".join(problems))
        self.problems = problems


def _program_name() -> str:
    argv0 = sys.argv[0] if sys.argv and sys.argv[0] else "python"
    return os.path.basename(argv0) or "python"


def redact_url(raw: str) -> str:
    """The URL without any ``user:password@`` part."""
    try:
        u = urlsplit(raw)
    except ValueError:
        return "<unparsable url>"
    if "@" not in u.netloc:
        return raw
    return u._replace(netloc=u.netloc.rsplit("@", 1)[1]).geturl()


@dataclass(frozen=True)
class NatsConfig:
    """
    What a connection needs. Durations are seconds. There is deliberately no
    switch that turns verification off and no free-form option string: a
    parameter handed through a string can be dropped on the way to the driver,
    and a dropped CA turns a verified connection into one that does not verify.
    """

    url: str = ""
    """The broker, one ``nats://`` or ``tls://`` URL. The server certificate must carry its host name."""
    ca_file: str = ""
    """Server CA. When set the connection is TLS and trusts this file and nothing else
    (not the system store). A ``tls://`` URL requires it."""
    cert_file: str = ""
    key_file: str = ""
    """The workload certificate and key, both or neither. Read again for every new connection."""
    token_file: str = ""
    """The projected ServiceAccount token the callout accepts as the NATS auth token.
    Read again for every new connection."""
    name: str = field(default_factory=_program_name)
    """The connection name the broker shows."""
    connect_timeout: float = 5.0
    reconnect_wait: float = 1.0
    ping_interval: float = 30.0
    drain_timeout: float = 10.0
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    """Bounds the retry of the first connection. After it the driver reconnects without limit."""

    @property
    def tls(self) -> bool:
        return bool(self.ca_file)

    def __repr__(self) -> str:
        # Paths only, never contents; the URL is redacted in case it was given credentials.
        return (
            f"NatsConfig(url={redact_url(self.url)!r}, ca_file={self.ca_file!r}, "
            f"cert_file={self.cert_file!r}, key_file={self.key_file!r}, "
            f"token_file={self.token_file!r}, name={self.name!r})"
        )

    __str__ = __repr__

    def with_(self, **changes: object) -> NatsConfig:
        """A copy with some fields replaced."""
        return replace(self, **changes)  # type: ignore[arg-type]

    def problems(self) -> list[str]:
        """Every problem found, not just the first."""
        p: list[str] = []
        scheme = ""
        if not self.url:
            p.append("URL is required (NATS_URL)")
        else:
            if "," in self.url or " " in self.url:
                p.append(f'URL "{redact_url(self.url)}": exactly one URL is accepted (the broker\'s Service)')
            try:
                u = urlsplit(self.url)
                host = u.hostname
                _ = u.port  # raises ValueError on a bad port
            except ValueError:
                u = None
                host = None
            if u is None or not host:
                p.append("URL is not a nats:// or tls:// URL with a host")
            elif u.scheme not in ("nats", "tls"):
                p.append(f'URL scheme "{u.scheme}" is refused; only nats:// and tls:// are accepted')
            else:
                scheme = u.scheme
            if u is not None and "@" in u.netloc:
                p.append("credentials in the URL are refused; use the token file or the certificate")
        if scheme == "tls" and not self.ca_file:
            p.append(
                "a tls:// URL needs the server CA file (NATS_CA_FILE): "
                "verification has nothing to verify against without it"
            )
        if bool(self.cert_file) != bool(self.key_file):
            p.append("client certificate and key go together")
        cert = bool(self.cert_file or self.key_file)
        if cert and self.token_file:
            p.append("a certificate and a token file are two identities; give one")
        elif not cert and not self.token_file:
            p.append(
                "a credential is required: a certificate and key (NATS_CERT_FILE, NATS_KEY_FILE) "
                "or a token file (NATS_TOKEN_FILE)"
            )
        if self.cert_file and self.key_file and not self.ca_file:
            p.append(
                "a client certificate needs the server CA file: "
                "it is only ever sent over a verified connection"
            )
        if not all(
            math.isfinite(d) and d > 0
            for d in (self.connect_timeout, self.reconnect_wait, self.ping_interval, self.drain_timeout)
        ):
            p.append("connect_timeout, reconnect_wait, ping_interval and drain_timeout must be positive")
        rp = self.retry.problem()
        if rp:
            p.append(rp)
        return p

    def validate(self) -> None:
        """Raises :class:`ConfigError` listing every problem."""
        p = self.problems()
        if p:
            raise ConfigError(p)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> NatsConfig:
        """
        Builds a configuration from the contract's environment. The result is validated.
        """
        e = os.environ if env is None else env
        problems: list[str] = []

        def get(k: str) -> str:
            return e.get(k) or ""

        def parse(k: str, fallback: T, f: Callable[[str], T]) -> T:
            v = get(k)
            if not v:
                return fallback
            try:
                return f(v)
            except ValueError as err:
                problems.append(f'natsclient: {k}="{v}": {err}')
                return fallback

        d = cls()
        c = cls(
            url=get("NATS_URL"),
            ca_file=get("NATS_CA_FILE"),
            cert_file=get("NATS_CERT_FILE"),
            key_file=get("NATS_KEY_FILE"),
            token_file=get("NATS_TOKEN_FILE"),
            name=get("NATS_CLIENT_NAME") or d.name,
            connect_timeout=parse("NATS_CLIENT_CONNECT_TIMEOUT", d.connect_timeout, parse_duration),
            reconnect_wait=parse("NATS_CLIENT_RECONNECT_WAIT", d.reconnect_wait, parse_duration),
            ping_interval=parse("NATS_CLIENT_PING_INTERVAL", d.ping_interval, parse_duration),
            drain_timeout=parse("NATS_CLIENT_DRAIN_TIMEOUT", d.drain_timeout, parse_duration),
            retry=RetryPolicy(
                attempts=parse("NATS_CLIENT_RETRY_ATTEMPTS", d.retry.attempts, _int),
                max_delay=parse("NATS_CLIENT_RETRY_MAX_DELAY", d.retry.max_delay, parse_duration),
                budget=parse("NATS_CLIENT_RETRY_BUDGET", d.retry.budget, parse_duration),
            ),
        )
        if problems:
            raise ConfigError(problems)
        c.validate()
        return c


def _int(s: str) -> int:
    try:
        return int(s, 10)
    except ValueError:
        raise ValueError("not a whole number") from None


_UNIT = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "μs": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0}
_PART = re.compile(r"(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)")


def parse_duration(text: str) -> float:
    """
    A Go-style duration in seconds: ``500ms``, ``5s``, ``1m30s``, ``1.5h``.
    A plain ``0`` is accepted; a negative value or an empty string is not.
    """
    if text == "0":
        return 0.0
    pos, total = 0, 0.0
    while pos < len(text):
        m = _PART.match(text, pos)
        if not m:
            raise ValueError(f'"{text}" is not a duration (use 500ms, 5s, 1m30s)')
        total += float(m.group(1)) * _UNIT[m.group(2)]
        pos = m.end()
    if pos == 0:
        raise ValueError(f'"{text}" is not a duration (use 500ms, 5s, 1m30s)')
    return total
