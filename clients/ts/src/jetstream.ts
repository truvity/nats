import {
  AckPolicy,
  type ConsumerInfo,
  DeliverPolicy,
  DiscardPolicy,
  JetStreamApiError,
  type PubAck,
  ReplayPolicy,
  RetentionPolicy,
  StorageType,
  type StreamInfo,
} from "@nats-io/jetstream";
import { type MsgHdrs, nanos, headers as newHeaders } from "@nats-io/transport-node";
import type { NatsClient } from "./client.js";
import { injectTrace } from "./headers.js";
import { validatePublishSubject } from "./subject.js";

/** The JetStream defaults. A stream or consumer built by this package differs from the broker's own only where these say. */
export const DEFAULT_STREAM_REPLICAS = 1;
export const DEFAULT_STREAM_MAX_AGE_MS = 7 * 24 * 3_600_000;
export const DEFAULT_DUPLICATE_WINDOW_MS = 2 * 60_000;
export const DEFAULT_ACK_WAIT_MS = 30_000;
export const DEFAULT_MAX_DELIVER = 5;
export const DEFAULT_MAX_ACK_PENDING = 1000;
const PUBLISH_RETRY_ATTEMPTS = 3;
const PUBLISH_TIMEOUT_MS = 5_000;
// "stream name already in use with a different configuration"
const ERR_STREAM_NAME_IN_USE = 10058;

/**
 * Names a stream; unset fields take the defaults: file storage, limits
 * retention, discard old, one replica, seven days, a two-minute
 * de-duplication window. Run three replicas against a three-node broker by
 * setting `replicas`.
 */
export interface StreamSpec {
  name: string;
  subjects: string[];
  description?: string;
  replicas?: number;
  maxAgeMs?: number;
}

/**
 * Names a durable consumer; unset fields take the defaults: explicit
 * acknowledgement, 30s ack wait, 5 deliveries, 1000 unacknowledged, deliver all.
 */
export interface ConsumerSpec {
  stream: string;
  durable: string;
  filterSubject?: string;
  ackWaitMs?: number;
  maxDeliver?: number;
  maxAckPending?: number;
}

export interface PublishOptions {
  /** Makes a retried publish idempotent within the stream's de-duplication window. */
  msgId?: string;
  /** Written as the trace headers (lower case). */
  traceparent?: string;
  tracestate?: string;
  /** Extra headers, sent as given. */
  headers?: MsgHdrs;
  /** Waits for the acknowledgement this long. Default 5000. */
  timeoutMs?: number;
}

function validStreamSubject(s: string): void {
  // A stream may bind a wildcard subject; only the reserved space is refused.
  if (s === "" || s.startsWith("$")) {
    throw new Error(`natsclient: stream subject "${s}" is empty or in the broker's reserved space`);
  }
}

/** Creates the stream or brings an existing one to the spec. Returns the stream's info (the Go adapter returns a handle). */
export async function ensureStream(c: NatsClient, s: StreamSpec): Promise<StreamInfo> {
  if (!s.name || !s.subjects || s.subjects.length === 0) {
    throw new Error("natsclient: a stream needs a name and subjects");
  }
  for (const sub of s.subjects) validStreamSubject(sub);
  const cfg = {
    name: s.name,
    ...(s.description ? { description: s.description } : {}),
    subjects: s.subjects,
    retention: RetentionPolicy.Limits,
    storage: StorageType.File,
    discard: DiscardPolicy.Old,
    num_replicas: s.replicas || DEFAULT_STREAM_REPLICAS,
    max_age: nanos(s.maxAgeMs || DEFAULT_STREAM_MAX_AGE_MS),
    duplicate_window: nanos(DEFAULT_DUPLICATE_WINDOW_MS),
  };
  const jsm = await c.jetstreamManager();
  try {
    try {
      return await jsm.streams.add(cfg);
    } catch (err) {
      if (!(err instanceof JetStreamApiError) || err.apiError().err_code !== ERR_STREAM_NAME_IN_USE)
        throw err;
      return await jsm.streams.update(s.name, cfg);
    }
  } catch (err) {
    throw new Error(`natsclient: ensure stream ${s.name}: ${(err as Error).message}`, { cause: err });
  }
}

/** Creates the durable consumer or brings an existing one to the spec. Returns the consumer's info. */
export async function ensureConsumer(c: NatsClient, s: ConsumerSpec): Promise<ConsumerInfo> {
  if (!s.stream || !s.durable) throw new Error("natsclient: a consumer needs a stream and a durable name");
  const jsm = await c.jetstreamManager();
  try {
    return await jsm.consumers.add(s.stream, {
      durable_name: s.durable,
      ...(s.filterSubject ? { filter_subject: s.filterSubject } : {}),
      ack_policy: AckPolicy.Explicit,
      ack_wait: nanos(s.ackWaitMs || DEFAULT_ACK_WAIT_MS),
      max_deliver: s.maxDeliver || DEFAULT_MAX_DELIVER,
      max_ack_pending: s.maxAckPending || DEFAULT_MAX_ACK_PENDING,
      deliver_policy: DeliverPolicy.All,
      replay_policy: ReplayPolicy.Instant,
    });
  } catch (err) {
    throw new Error(`natsclient: ensure consumer ${s.stream}/${s.durable}: ${(err as Error).message}`, {
      cause: err,
    });
  }
}

/**
 * Sends `data` to a JetStream subject and waits for the stream's
 * acknowledgement, retrying a missing responder (a stream still being
 * created or a leader election) a few times.
 */
export async function publish(
  c: NatsClient,
  subject: string,
  data: Uint8Array | string,
  o: PublishOptions = {},
): Promise<PubAck> {
  validatePublishSubject(subject);
  const h = newHeaders();
  if (o.headers) {
    for (const [k, vs] of o.headers) for (const v of vs) h.append(k, v);
  }
  injectTrace(h, o.traceparent ?? "", o.tracestate ?? "");
  return c.jetstream().publish(subject, data, {
    headers: h,
    retries: PUBLISH_RETRY_ATTEMPTS,
    timeout: o.timeoutMs ?? PUBLISH_TIMEOUT_MS,
    ...(o.msgId ? { msgID: o.msgId } : {}),
  });
}
