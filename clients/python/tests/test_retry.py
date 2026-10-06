from __future__ import annotations

import asyncio
import ssl

import pytest
from nats import errors as nats_errors

from truvity_nats import CredentialError, RetryPolicy, backoff, is_retryable, retry

FAST = RetryPolicy(attempts=5, initial_delay=0.001, max_delay=0.002, budget=10.0)


def chained(outer: Exception, cause: BaseException) -> Exception:
    outer.__cause__ = cause
    return outer


@pytest.mark.parametrize(
    "err",
    [
        ConnectionRefusedError(),
        ConnectionResetError(),
        BrokenPipeError(),
        TimeoutError(),
        asyncio.TimeoutError(),
        EOFError(),
        nats_errors.NoServersError(),
        nats_errors.TimeoutError(),
        nats_errors.UnexpectedEOF(),
        nats_errors.ConnectionClosedError(),
        OSError(111, "Connect call failed"),
        chained(RuntimeError("wrapped"), ConnectionRefusedError()),
    ],
)
def test_retryable(err: Exception) -> None:
    assert is_retryable(err)


@pytest.mark.parametrize(
    "err",
    [
        None,
        ValueError("nope"),
        nats_errors.AuthorizationError(),
        nats_errors.Error("nats: 'Authorization Violation'"),
        nats_errors.Error("nats: 'User Authentication Expired'"),
        nats_errors.Error("nats: Permissions Violation for Publish to x"),
        nats_errors.SecureConnWantedError(),
        nats_errors.SecureConnRequiredError(),
        CredentialError("token file unreadable"),
        ssl.SSLError("tlsv1 alert unknown ca"),
        ssl.SSLCertVerificationError("certificate verify failed"),
        ssl.CertificateError("hostname mismatch"),
        chained(ConnectionResetError(), ssl.SSLError("handshake")),
        chained(OSError("x"), ssl.SSLCertVerificationError("unable to get local issuer certificate")),
        asyncio.CancelledError(),
    ],
)
def test_never_retryable(err: BaseException | None) -> None:
    assert not is_retryable(err)


def test_backoff_is_bounded_by_a_doubling_ceiling() -> None:
    p = RetryPolicy(attempts=9, initial_delay=0.1, max_delay=0.5, budget=60)
    for attempt, ceil in [(1, 0.1), (2, 0.2), (3, 0.4), (4, 0.5), (9, 0.5), (200, 0.5)]:
        for _ in range(50):
            assert 0 <= backoff(p, attempt) <= ceil


def test_retry_repeats_until_success() -> None:
    calls = 0

    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionRefusedError
        return "ok"

    assert asyncio.run(retry(FAST, fn)) == "ok"
    assert calls == 3


def test_retry_stops_at_attempts() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise ConnectionRefusedError

    with pytest.raises(ConnectionRefusedError):
        asyncio.run(retry(RetryPolicy(attempts=3, initial_delay=0.001, max_delay=0.002, budget=10), fn))
    assert calls == 3


def test_retry_does_not_repeat_a_non_retryable_error() -> None:
    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise ssl.SSLCertVerificationError("certificate verify failed")

    with pytest.raises(ssl.SSLCertVerificationError):
        asyncio.run(retry(FAST, fn))
    assert calls == 1


def test_retry_stops_at_the_budget() -> None:
    now = 0.0
    slept: list[float] = []

    def clock() -> float:
        return now

    async def sleep(d: float) -> None:
        nonlocal now
        slept.append(d)
        now += 1.0  # every wait costs a second on the fake clock

    calls = 0

    async def fn() -> None:
        nonlocal calls
        calls += 1
        raise ConnectionRefusedError

    p = RetryPolicy(attempts=100, initial_delay=0.5, max_delay=0.5, budget=3.2)
    with pytest.raises(ConnectionRefusedError):
        asyncio.run(retry(p, fn, sleep=sleep, clock=clock))
    assert calls == len(slept) + 1 <= 5


def test_retry_rejects_a_bad_policy() -> None:
    async def fn() -> None:  # pragma: no cover
        return None

    with pytest.raises(ValueError, match="retry:"):
        asyncio.run(retry(RetryPolicy(attempts=0), fn))
