"""Retry of the first connection: connection-class failures only."""

from __future__ import annotations

import asyncio
import random
import ssl
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from nats import errors as nats_errors

from .config import RetryPolicy

T = TypeVar("T")


class CredentialError(nats_errors.Error):
    """A credential file (CA, certificate, key, token) could not be read or used.

    It subclasses the driver's error class because the driver's reconnect loop
    keeps trying after exactly that class (a file may reappear), and it is never
    retried on the first connection.
    """


def backoff(policy: RetryPolicy, attempt: int) -> float:
    """Full jitter under a doubling ceiling. ``attempt`` is 1 for the first retry."""
    ceil = min(policy.max_delay, policy.initial_delay * 2 ** min(attempt - 1, 30))
    return random.uniform(0.0, ceil)


async def retry(
    policy: RetryPolicy,
    fn: Callable[[], Awaitable[T]],
    *,
    sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> T:
    """
    Runs ``fn``, repeating it while it fails with a retryable error (see
    :func:`is_retryable`), stopping at ``attempts`` or ``budget``, whichever is
    first. ``fn`` must be safe to run again. Cancellation is never retried.
    """
    problem = policy.problem()
    if problem:
        raise ValueError(f"natsclient: {problem}")
    start = clock()
    attempt = 1
    while True:
        try:
            return await fn()
        except Exception as err:
            if not is_retryable(err) or attempt >= policy.attempts:
                raise
            delay = backoff(policy, attempt)
            if clock() - start + delay > policy.budget:
                raise
            await sleep(delay)
            attempt += 1


# Messages that mean a second try cannot help, for errors the driver reports as plain text.
_NEVER_TEXT = (
    "authorization violation",
    "authentication",
    "permissions violation",
    "certificate",
    "secure connection",
)

_NEVER_TYPES: tuple[type[BaseException], ...] = (
    nats_errors.AuthorizationError,
    nats_errors.InvalidUserCredentialsError,
    nats_errors.SecureConnRequiredError,
    nats_errors.SecureConnWantedError,
    nats_errors.SecureConnFailedError,
    CredentialError,
    ssl.SSLError,  # includes SSLCertVerificationError (unknown CA, host name mismatch)
    ssl.CertificateError,
    asyncio.CancelledError,
)

_RETRY_TYPES: tuple[type[BaseException], ...] = (
    nats_errors.NoServersError,
    nats_errors.TimeoutError,
    nats_errors.ConnectionClosedError,
    nats_errors.StaleConnectionError,  # includes UnexpectedEOF
    asyncio.TimeoutError,
    TimeoutError,
    EOFError,  # includes asyncio.IncompleteReadError
    OSError,  # connection refused/reset, broken pipe, a name that does not resolve yet
)


def _chain(err: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    seen: set[int] = set()
    cur: BaseException | None = err
    while cur is not None and id(cur) not in seen and len(out) < 8:
        seen.add(id(cur))
        out.append(cur)
        cur = cur.__cause__ or cur.__context__
    return out


def is_retryable(err: BaseException | None) -> bool:
    """
    Reports whether ``err`` means the broker was not reachable (down, restarting,
    not accepting yet), which a later try can fix.

    Never retryable: an authorization or authentication failure, a certificate
    verification or TLS handshake failure (a second try cannot fix either), an
    unreadable credential file, a permissions violation, and cancellation.
    """
    if err is None:
        return False
    errs = _chain(err)
    for e in errs:
        if isinstance(e, _NEVER_TYPES):
            return False
        msg = str(e).lower()
        if any(t in msg for t in _NEVER_TEXT):
            return False
    return any(isinstance(e, _RETRY_TYPES) for e in errs)
