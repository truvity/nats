export { type ConnectOptions, driverOptions, HEALTH_TIMEOUT_MS, type Logger, NatsClient } from "./client.js";
export {
  ConfigError,
  configFromEnv,
  defaultConfig,
  defaultRetryPolicy,
  type NatsConfig,
  parseDuration,
  type RetryPolicy,
  redactConfig,
  redactUrl,
  validateConfig,
} from "./config.js";
export {
  extractTrace,
  HEADER_TRACEPARENT,
  HEADER_TRACESTATE,
  HeaderCarrier,
  injectTrace,
} from "./headers.js";
export {
  type ConsumerSpec,
  DEFAULT_ACK_WAIT_MS,
  DEFAULT_DUPLICATE_WINDOW_MS,
  DEFAULT_MAX_ACK_PENDING,
  DEFAULT_MAX_DELIVER,
  DEFAULT_STREAM_MAX_AGE_MS,
  DEFAULT_STREAM_REPLICAS,
  type PublishOptions,
  type StreamSpec,
} from "./jetstream.js";
export { backoffMs, isRetryable, retry, validateRetryPolicy } from "./retry.js";
export {
  accountForNamespace,
  isValidPublishSubject,
  MAX_SUBJECT_LENGTH,
  validatePublishSubject,
} from "./subject.js";
