"""The conformance suite (clients/conformance/cases.txt): one test per case, named
``test_<case with _ for ->``, run against the broker that clients/conformance/nats-broker.sh starts.

Without NATS_CLIENTS_URL every test skips, unless NATS_CLIENTS=required, which turns a missing
broker into a failure. clients/conformance/guard.sh python <junit xml> fails unless every case ran
and passed.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import os
import shutil
import time
import urllib.request
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import pytest
from nats.aio.client import Client as NATS
from nats.js import api

from truvity_nats import (
    ConfigError,
    ConsumerSpec,
    NatsClient,
    NatsConfig,
    RetryPolicy,
    StreamSpec,
    account_for_namespace,
    extract_trace,
    inject_trace,
    valid_publish_subject,
)
from truvity_nats.headers import HeaderCarrier

P = ParamSpec("P")
R = TypeVar("R")

CONFORMANCE = Path(__file__).resolve().parents[2] / "conformance"
CERTS = [
    "ca.crt",
    "other-ca.crt",
    "api.crt",
    "api.key",
    "worker.crt",
    "worker.key",
    "foreign.crt",
    "foreign.key",
]


def aio(fn: Callable[P, Awaitable[R]]) -> Callable[P, R]:
    """Runs an async test body on its own event loop (no pytest plugin needed)."""

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return asyncio.run(fn(*args, **kwargs))  # type: ignore[arg-type]

    wrapper.__signature__ = inspect.signature(fn)  # type: ignore[attr-defined]
    return wrapper


@dataclass(frozen=True)
class Env:
    url: str
    tls_url: str
    tls_url_ip: str
    monitor: str
    dir: str
    container: str
    trust: str
    run: str

    def file(self, name: str) -> str:
        return os.path.join(self.dir, name)

    def subj(self, s: str) -> str:
        return f"conformance.{self.run}.{s}"

    def work(self, tmp: Path, names: list[str] | None = None) -> Path:
        """Copies files into a per-test directory so a test can rotate them."""
        for n in names or []:
            shutil.copyfile(self.file(n), tmp / n)
            (tmp / n).chmod(0o600)
        return tmp

    def cert_config(self, name: str, d: Path, cert: str) -> NatsConfig:
        return NatsConfig(
            url=self.tls_url,
            ca_file=str(d / "ca.crt"),
            cert_file=str(d / f"{cert}.crt"),
            key_file=str(d / f"{cert}.key"),
            name=name,
            reconnect_wait=0.2,
        )

    def token_config(self, name: str, token_file: Path) -> NatsConfig:
        return NatsConfig(url=self.url, token_file=str(token_file), name=name, reconnect_wait=0.2)

    @property
    def api_id(self) -> str:
        return f"spiffe://{self.trust}/ns/shop/sa/api"

    @property
    def worker_id(self) -> str:
        return f"spiffe://{self.trust}/ns/shop/sa/worker"


@pytest.fixture(scope="module")
def env() -> Env:
    url = os.environ.get("NATS_CLIENTS_URL", "")
    if not url:
        if os.environ.get("NATS_CLIENTS") == "required":
            pytest.fail("NATS_CLIENTS=required but NATS_CLIENTS_URL is not set: no broker")
        pytest.skip("NATS_CLIENTS_URL not set: no broker")
    e = os.environ.get
    return Env(
        url=url,
        tls_url=e("NATS_CLIENTS_TLS_URL", ""),
        tls_url_ip=e("NATS_CLIENTS_TLS_URL_IP", ""),
        monitor=e("NATS_CLIENTS_MONITOR", ""),
        dir=e("NATS_CLIENTS_DIR", ""),
        container=e("NATS_CLIENTS_CONTAINER", ""),
        trust=e("NATS_CLIENTS_TRUST_DOMAIN", ""),
        # Subjects and streams are unique per run: a broker may be reused.
        run=f"r{time.time_ns()}",
    )


# --- helpers -------------------------------------------------------------------------------------


async def eventually(
    cond: Callable[[], Any], timeout: float, interval: float, msg: str = "condition not met"
) -> None:
    """Polls ``cond`` (sync or async) until it is truthy; failures of the poll are not fatal."""
    deadline = time.monotonic() + timeout
    while True:
        with suppress(Exception):
            r = cond()
            if inspect.isawaitable(r):
                r = await r
            if r:
                return
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out after {timeout}s: {msg}")
        await asyncio.sleep(interval)


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, b""


async def get(url: str) -> tuple[int, bytes]:
    # Off the event loop: a blocked loop would also block a client that is trying to reconnect.
    return await asyncio.to_thread(_get, url)


async def is_healthy(c: NatsClient) -> bool:
    await c.health()
    return True


async def connz(e: Env) -> list[dict[str, Any]]:
    """Who the broker says is connected, with the identity it authorized."""
    status, body = await get(e.monitor + "/connz?auth=true&state=open")
    assert status == 200, body
    conns: list[dict[str, Any]] = json.loads(body).get("connections") or []
    return conns


async def find_conn(e: Env, name: str) -> dict[str, Any] | None:
    return next((c for c in await connz(e) if c.get("name") == name), None)


async def wait_conn(
    e: Env, name: str, ok: Callable[[dict[str, Any]], bool] = lambda _: True
) -> dict[str, Any]:
    last: dict[str, Any] | None = None

    async def cond() -> bool:
        nonlocal last
        last = await find_conn(e, name)
        return last is not None and ok(last)

    await eventually(cond, 30, 0.1, f"connection {name!r}: last seen {last}")
    assert last is not None
    return last


async def restart(e: Env) -> None:
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "restart",
        "-t",
        "1",
        e.container,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    assert proc.returncode == 0, out.decode()

    async def healthy() -> bool:
        status, _ = await get(e.monitor + "/healthz")
        return status == 200

    await eventually(healthy, 30, 0.1, "the broker came back")

    # Up is not ready: the callout reconnects a moment later, and until it does the broker
    # refuses every token. Wait until one is let in.
    async def token_let_in() -> bool:
        nc = NATS()
        try:
            await nc.connect(e.url, token="token-shop-api", connect_timeout=2, allow_reconnect=False)
        except Exception:
            return False
        await nc.close()
        return True

    await eventually(token_let_in, 30, 0.2, "the callout came back")


@asynccontextmanager
async def opened(cfg: NatsConfig) -> AsyncIterator[NatsClient]:
    c = await asyncio.wait_for(NatsClient.connect(cfg), 20)
    try:
        yield c
    finally:
        with suppress(Exception):
            await asyncio.wait_for(c.close(), 5)


async def connect_fails(cfg: NatsConfig) -> Exception:
    cfg = cfg.with_(retry=RetryPolicy(attempts=3, budget=2.0))
    start = time.monotonic()
    try:
        c = await asyncio.wait_for(NatsClient.connect(cfg), 20)
    except Exception as err:
        # A verification or authorization failure is not retried.
        assert time.monotonic() - start < 5
        return err
    with suppress(Exception):
        await c.close()
    raise AssertionError("the connection was accepted")


def write_token(path: Path, token: str) -> Path:
    path.write_text(token)
    path.chmod(0o600)
    return path


def vectors(name: str) -> Any:
    return json.loads((CONFORMANCE / "vectors" / name).read_text())


# --- the cases, in cases.txt order ---------------------------------------------------------------


@aio
async def test_verify_full_connects(env: Env, tmp_path: Path) -> None:
    d = env.work(tmp_path, CERTS)
    async with opened(env.cert_config("verify-full-connects", d, "api")) as c:
        await c.health()
        got = await wait_conn(env, "verify-full-connects")
        assert got["authorized_user"] == env.api_id
        assert got["account"] == "shop"


@aio
async def test_rejects_unknown_ca(env: Env, tmp_path: Path) -> None:
    d = env.work(tmp_path, CERTS)
    cfg = env.cert_config("rejects-unknown-ca", d, "api").with_(ca_file=str(d / "other-ca.crt"))
    assert "certificate" in str(await connect_fails(cfg)).lower()


@aio
async def test_rejects_hostname_mismatch(env: Env, tmp_path: Path) -> None:
    d = env.work(tmp_path, CERTS)
    # The server certificate carries only `localhost`.
    cfg = env.cert_config("rejects-hostname-mismatch", d, "api").with_(url=env.tls_url_ip)
    assert "certificate" in str(await connect_fails(cfg)).lower()


@aio
async def test_rejects_foreign_client_cert(env: Env, tmp_path: Path) -> None:
    d = env.work(tmp_path, CERTS)
    await connect_fails(env.cert_config("rejects-foreign-client-cert", d, "foreign"))


@aio
async def test_refuses_unsafe_config(env: Env, tmp_path: Path) -> None:
    cfg = NatsConfig(
        url="tls://localhost:4222", cert_file="/x.crt", token_file="/token"
    )  # no CA, no key, two identities
    with pytest.raises(ConfigError) as ei:
        cfg.validate()
    # Every problem at once, not the first.
    for want in ("server CA", "go together", "two identities"):
        assert want in str(ei.value)
    with pytest.raises(ConfigError, match="scheme"):
        NatsConfig(url="http://localhost:4222", token_file="/token").validate()
    with pytest.raises(ConfigError, match="credentials in the URL") as ei2:
        NatsConfig(url="nats://user:pw@localhost:4222", token_file="/token").validate()
    assert "pw" not in str(ei2.value)
    assert "pw" not in repr(NatsConfig(url="nats://user:pw@localhost:4222", token_file="/token"))
    # A broker that does not offer TLS is not talked to in the clear when a CA was given.
    d = env.work(tmp_path, CERTS)
    token = write_token(d / "token", "token-shop-api")
    plain = env.token_config("refuses-unsafe-config", token).with_(ca_file=str(d / "ca.crt"))
    await connect_fails(plain)


@aio
async def test_token_connects(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api\n")
    async with opened(env.token_config("token-connects", token)) as c:
        await c.health()
        got = await wait_conn(env, "token-connects")
        assert got["account"] == "shop"


@aio
async def test_token_account_mapping(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-other-app")
    async with opened(env.token_config("token-account-mapping", token)):
        got = await wait_conn(env, "token-account-mapping")
        assert got["account"] == account_for_namespace("other", ["shop", "other"])


@aio
async def test_rejects_unmapped_namespace(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-kube-system")
    await connect_fails(env.token_config("rejects-unmapped-namespace", token))
    unknown = write_token(tmp_path / "unknown", "not-a-token")
    await connect_fails(env.token_config("rejects-unknown-token", unknown))


@aio
async def test_client_cert_rotation(env: Env, tmp_path: Path) -> None:
    d = env.work(tmp_path, CERTS)
    async with opened(env.cert_config("client-cert-rotation", d, "api")) as c:
        assert (await wait_conn(env, "client-cert-rotation"))["authorized_user"] == env.api_id
        # The files change under the running process, as cert-manager renews them.
        for ext in ("crt", "key"):
            shutil.copyfile(env.file(f"worker.{ext}"), d / f"api.{ext}")
        await restart(env)
        got = await wait_conn(
            env, "client-cert-rotation", lambda ci: ci.get("authorized_user") == env.worker_id
        )
        assert got["authorized_user"] == env.worker_id
        await eventually(lambda: is_healthy(c), 20, 0.1, "healthy again")


@aio
async def test_token_file_rotation(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with opened(env.token_config("token-file-rotation", token)) as c:
        first = await wait_conn(env, "token-file-rotation")
        write_token(token, "token-shop-worker")
        await restart(env)
        got = await wait_conn(
            env,
            "token-file-rotation",
            lambda ci: (
                ci.get("account") == "shop"
                and bool(ci.get("authorized_user"))
                and ci["authorized_user"] != first.get("authorized_user")
            ),
        )
        assert got["authorized_user"] != first.get("authorized_user")
        assert got["account"] == "shop"
        await eventually(lambda: is_healthy(c), 20, 0.1, "healthy again")


@aio
async def test_reconnects_after_broker_restart(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with opened(env.token_config("reconnects-after-broker-restart", token)) as c:
        got: list[bytes] = []

        async def on_msg(m: Any) -> None:
            got.append(m.data)

        sub = await c.conn.subscribe("conformance.restart", cb=on_msg)
        await c.conn.flush()
        await restart(env)

        # The subscription is restored by the driver; a publish after the reconnect reaches it
        # without the caller doing anything.
        async def delivered() -> bool:
            await c.health()
            await c.conn.publish("conformance.restart", b"x")
            await c.conn.flush()
            await asyncio.sleep(0.1)
            return bool(got)

        await eventually(delivered, 30, 0.2, "a message after the restart")
        assert sub.delivered > 0


@aio
async def test_drain_delivers_inflight(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with (
        opened(env.token_config("drain-sub", token)) as sub,
        opened(env.token_config("drain-pub", token)) as pub,
    ):
        n = 200
        got = 0

        async def on_msg(_m: Any) -> None:
            nonlocal got
            await asyncio.sleep(0.001)
            got += 1

        await sub.conn.subscribe("conformance.drain", cb=on_msg)
        await sub.conn.flush()
        for _ in range(n):
            await pub.conn.publish("conformance.drain", b"x")
        await pub.conn.flush()
        # Let the broker's copies reach the subscriber before the drain starts.
        await eventually(
            lambda: sub.conn.stats["in_msgs"] >= n, 10, 0.01, "all messages delivered to the client"
        )
        await asyncio.wait_for(sub.close(), 20)
        assert got == n, "every message delivered to the subscriber before the drain started is handled"
        assert sub.conn.is_closed


@aio
async def test_jetstream_defaults(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with opened(env.token_config("jetstream-defaults", token)) as c:
        name = f"DEFAULTS{time.time_ns()}"
        subjects = [env.subj("defaults.>")]
        info = (await c.ensure_stream(StreamSpec(name=name, subjects=subjects))).config
        assert info.storage == api.StorageType.FILE
        assert info.retention == api.RetentionPolicy.LIMITS
        assert info.discard == api.DiscardPolicy.OLD
        assert info.num_replicas == 1
        assert info.max_age == 7 * 24 * 3600
        assert info.duplicate_window == 120
        # Idempotent, and a changed spec updates the stream.
        await c.ensure_stream(StreamSpec(name=name, subjects=subjects))
        changed = await c.ensure_stream(StreamSpec(name=name, subjects=subjects, max_age=3600))
        assert changed.config.max_age == 3600

        filt = env.subj("defaults.a")
        cons = await c.ensure_consumer(ConsumerSpec(stream=name, durable="worker", filter_subject=filt))
        cc = cons.config
        assert cc.durable_name == "worker"
        assert cc.ack_policy == api.AckPolicy.EXPLICIT
        assert cc.ack_wait == 30
        assert cc.max_deliver == 5
        assert cc.max_ack_pending == 1000
        assert cc.deliver_policy == api.DeliverPolicy.ALL
        assert cc.filter_subject == filt
        await c.ensure_consumer(ConsumerSpec(stream=name, durable="worker", filter_subject=filt))


@aio
async def test_jetstream_publish_ack_dedup(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with opened(env.token_config("jetstream-publish-ack-dedup", token)) as c:
        name = f"DEDUP{time.time_ns()}"
        await c.ensure_stream(StreamSpec(name=name, subjects=[env.subj("dedup.>")]))
        first = await c.publish(env.subj("dedup.a"), b"1", msg_id="m-1")
        assert not first.duplicate
        second = await c.publish(env.subj("dedup.a"), b"1", msg_id="m-1")
        assert second.duplicate
        assert second.seq == first.seq
        # A wildcard subject is refused before it is sent.
        with pytest.raises(ValueError):
            await c.publish(env.subj("dedup.*"), b"1")


@aio
async def test_cert_identity_publish_limited(env: Env, tmp_path: Path) -> None:
    # The stream is made by a token client; the certificate identity may only publish.
    d = env.work(tmp_path, CERTS)
    token = write_token(d / "token", "token-shop-api")
    async with opened(env.token_config("cert-limited-admin", token)) as admin:
        name = f"LIMITED{time.time_ns()}"
        await admin.ensure_stream(
            StreamSpec(name=name, subjects=[env.subj("limited.>"), f"forbidden.{env.run}.>"])
        )
        async with opened(env.cert_config("cert-identity-publish-limited", d, "api")) as c:
            ack = await c.publish(env.subj("limited.ok"), b"x")
            assert ack.stream == name
            with pytest.raises(Exception):  # noqa: B017 - a timeout or a permissions error, both mean refused
                await c.publish(f"forbidden.{env.run}.no", b"x", timeout=2)


@aio
async def test_propagates_trace_headers(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with opened(env.token_config("propagates-trace-headers", token)) as c:
        name = f"TRACE{time.time_ns()}"
        await c.ensure_stream(StreamSpec(name=name, subjects=[env.subj("trace.>")]))
        tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ack = await c.publish(env.subj("trace.a"), b"x", traceparent=tp, tracestate="k=v")

        # What is on the wire: the lower-case keys, exactly, read by the driver without the adapter.
        raw = await c.js.get_msg(name, seq=ack.seq)
        headers = raw.headers or {}
        assert "traceparent" in headers, "traceparent is written lower-case"
        assert "Traceparent" not in headers, "and not in the HTTP-canonical spelling"
        got_tp, got_ts = extract_trace(headers)
        assert got_tp == tp
        assert got_ts == "k=v"

        # A message another language wrote with the canonical spelling is still read.
        other = {"Traceparent": tp, "Tracestate": "a=b"}
        assert extract_trace(other) == (tp, "a=b")
        # Setting replaces every spelling.
        inject_trace(other, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00", "")
        assert list(HeaderCarrier(other).keys()) == ["traceparent"]


@aio
async def test_health_check(env: Env, tmp_path: Path) -> None:
    token = write_token(tmp_path / "token", "token-shop-api")
    async with opened(env.token_config("health-check", token)) as c:
        start = time.monotonic()
        await c.health()
        assert time.monotonic() - start < 2
        await asyncio.wait_for(c.close(), 10)
        with pytest.raises(Exception):  # noqa: B017 - a closed connection is not healthy
            await c.health()


def test_account_for_namespace_vectors(env: Env) -> None:
    v = vectors("account-for-namespace.json")
    assert v["cases"]
    for case in v["cases"]:
        ns = case["namespace"]
        if case.get("error"):
            with pytest.raises(ValueError):
                account_for_namespace(ns, v["projectAccounts"])
        else:
            assert account_for_namespace(ns, v["projectAccounts"]) == case["account"], ns


def test_subject_vectors(env: Env) -> None:
    v = vectors("subjects.json")
    assert v["valid"]
    for s in v["valid"]:
        valid_publish_subject(s)
    for s in v["invalid"]:
        with pytest.raises(ValueError):
            valid_publish_subject(s)
    valid_publish_subject("a" * (v["tooLong"] - 1))
    with pytest.raises(ValueError):
        valid_publish_subject("a" * v["tooLong"])
