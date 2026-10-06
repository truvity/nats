"""NATS client adapter for the shared broker. See the client contract in clients/README.md."""

from .client import HEALTH_TIMEOUT, NatsClient, NatsClientError
from .config import ConfigError, NatsConfig, RetryPolicy, parse_duration, redact_url
from .headers import (
    HEADER_TRACEPARENT,
    HEADER_TRACESTATE,
    HeaderCarrier,
    extract_trace,
    inject_trace,
)
from .jetstream import ConsumerSpec, StreamSpec
from .retry import CredentialError, backoff, is_retryable, retry
from .subject import (
    NamespaceError,
    SubjectError,
    account_for_namespace,
    is_valid_publish_subject,
    valid_publish_subject,
)

__all__ = [
    "HEADER_TRACEPARENT",
    "HEADER_TRACESTATE",
    "HEALTH_TIMEOUT",
    "ConfigError",
    "ConsumerSpec",
    "CredentialError",
    "HeaderCarrier",
    "NamespaceError",
    "NatsClient",
    "NatsClientError",
    "NatsConfig",
    "RetryPolicy",
    "StreamSpec",
    "SubjectError",
    "account_for_namespace",
    "backoff",
    "extract_trace",
    "inject_trace",
    "is_retryable",
    "is_valid_publish_subject",
    "parse_duration",
    "redact_url",
    "retry",
    "valid_publish_subject",
]
