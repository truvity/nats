"""JetStream defaults: ensure a stream or consumer, publish with acknowledgement."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass

from nats.js import JetStreamContext, api
from nats.js import errors as js_errors

from .headers import inject_trace
from .subject import valid_publish_subject

DEFAULT_STREAM_REPLICAS = 1
DEFAULT_STREAM_MAX_AGE = 7 * 24 * 3600.0
DEFAULT_DUPLICATE_WINDOW = 120.0
DEFAULT_ACK_WAIT = 30.0
DEFAULT_MAX_DELIVER = 5
DEFAULT_MAX_ACK_PENDING = 1000
PUBLISH_RETRY_ATTEMPTS = 3
PUBLISH_RETRY_WAIT = 0.25
DEFAULT_PUBLISH_TIMEOUT = 5.0


@dataclass(frozen=True)
class StreamSpec:
    """A stream. The rest is fixed: file storage, limits retention, discard old and a 2 minute
    de-duplication window. Run three replicas against a three-node broker by setting ``replicas``."""

    name: str
    subjects: tuple[str, ...] | list[str]
    description: str = ""
    replicas: int = DEFAULT_STREAM_REPLICAS
    max_age: float = DEFAULT_STREAM_MAX_AGE
    """Seconds."""


@dataclass(frozen=True)
class ConsumerSpec:
    """A durable consumer: explicit acknowledgement, deliver all, instant replay."""

    stream: str
    durable: str
    filter_subject: str = ""
    ack_wait: float = DEFAULT_ACK_WAIT
    """Seconds."""
    max_deliver: int = DEFAULT_MAX_DELIVER
    max_ack_pending: int = DEFAULT_MAX_ACK_PENDING


def _valid_stream_subject(s: str) -> None:
    # A stream may bind a wildcard subject; only the reserved space is refused.
    if not s or s.startswith("$"):
        raise ValueError(f'natsclient: stream subject "{s}" is empty or in the broker\'s reserved space')


async def ensure_stream(js: JetStreamContext, spec: StreamSpec) -> api.StreamInfo:
    """Creates the stream or brings an existing one to the spec."""
    if not spec.name or not spec.subjects:
        raise ValueError("natsclient: a stream needs a name and subjects")
    for s in spec.subjects:
        _valid_stream_subject(s)
    if spec.replicas < 1 or spec.max_age <= 0:
        raise ValueError("natsclient: a stream needs replicas >= 1 and a positive max_age")
    cfg = api.StreamConfig(
        name=spec.name,
        description=spec.description or None,
        subjects=list(spec.subjects),
        retention=api.RetentionPolicy.LIMITS,
        storage=api.StorageType.FILE,
        discard=api.DiscardPolicy.OLD,
        num_replicas=spec.replicas,
        max_age=spec.max_age,
        duplicate_window=DEFAULT_DUPLICATE_WINDOW,
    )
    try:
        try:
            return await js.update_stream(cfg)
        except js_errors.NotFoundError:
            return await js.add_stream(cfg)
    except js_errors.Error as err:
        raise js_errors.Error(f"natsclient: ensure stream {spec.name}: {err}") from err


async def ensure_consumer(js: JetStreamContext, spec: ConsumerSpec) -> api.ConsumerInfo:
    """Creates the durable consumer or brings an existing one to the spec."""
    if not spec.stream or not spec.durable:
        raise ValueError("natsclient: a consumer needs a stream and a durable name")
    if spec.ack_wait <= 0 or spec.max_deliver == 0 or spec.max_ack_pending < 1:
        raise ValueError("natsclient: a consumer needs a positive ack_wait, max_deliver and max_ack_pending")
    cfg = api.ConsumerConfig(
        durable_name=spec.durable,
        filter_subject=spec.filter_subject or None,
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=spec.ack_wait,
        max_deliver=spec.max_deliver,
        max_ack_pending=spec.max_ack_pending,
        deliver_policy=api.DeliverPolicy.ALL,
        replay_policy=api.ReplayPolicy.INSTANT,
    )
    try:
        return await js.add_consumer(spec.stream, cfg)  # create or update, by the durable name
    except js_errors.Error as err:
        raise js_errors.Error(f"natsclient: ensure consumer {spec.stream}/{spec.durable}: {err}") from err


async def publish(
    js: JetStreamContext,
    subject: str,
    data: bytes,
    *,
    msg_id: str = "",
    traceparent: str = "",
    tracestate: str = "",
    headers: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_PUBLISH_TIMEOUT,
) -> api.PubAck:
    """
    Sends ``data`` to a JetStream subject and waits for the stream's acknowledgement,
    retrying a missing responder (a stream still being created or a leader election)
    a few times. ``msg_id`` makes a retried publish idempotent within the stream's
    de-duplication window.
    """
    valid_publish_subject(subject)
    hdr: dict[str, str] = dict(headers or {})
    inject_trace(hdr, traceparent, tracestate)
    if msg_id:
        hdr["Nats-Msg-Id"] = msg_id
    for attempt in range(1, PUBLISH_RETRY_ATTEMPTS + 1):
        try:
            return await js.publish(subject, data, timeout=timeout, headers=dict(hdr) or None)
        except js_errors.NoStreamResponseError:
            if attempt >= PUBLISH_RETRY_ATTEMPTS:
                raise
            await asyncio.sleep(PUBLISH_RETRY_WAIT)
    raise AssertionError("unreachable")  # pragma: no cover
