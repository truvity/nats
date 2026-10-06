import { Match, type MsgHdrs, headers as newHeaders } from "@nats-io/transport-node";

/**
 * The W3C trace-context header names, as written on the wire: lower case.
 *
 * The trap: NATS headers are HTTP-like, and some drivers canonicalise a key
 * ("traceparent" becomes "Traceparent") while others send it as given. A
 * reader that looks a key up exactly then misses what another language wrote.
 * The contract is: write the lower-case name exactly, read case-insensitively.
 * nats.js canonicalises on `Match.CanonicalMIME` only, so this module always
 * passes `Match.Exact` to write and `Match.IgnoreCase` to read.
 */
export const HEADER_TRACEPARENT = "traceparent";
export const HEADER_TRACESTATE = "tracestate";

/**
 * A nats.js header set with the contract's casing. `get`, `set` and `keys`
 * satisfy OpenTelemetry's TextMapGetter/Setter shape
 * (`propagation.inject(ctx, carrier, { set: (c, k, v) => c.set(k, v) })`)
 * without this package importing OpenTelemetry.
 */
export class HeaderCarrier {
  constructor(readonly headers: MsgHdrs = newHeaders()) {}

  /** The first value of `key`, matched case-insensitively; "" when absent. */
  get(key: string): string {
    return this.headers.get(key, Match.IgnoreCase);
  }

  /** Replaces every spelling of `key` with one lower-case entry. An empty value removes the key. */
  set(key: string, value: string): void {
    this.headers.delete(key, Match.IgnoreCase);
    if (value !== "") this.headers.set(key.toLowerCase(), value, Match.Exact);
  }

  keys(): string[] {
    return this.headers.keys();
  }
}

/** Writes the trace headers, replacing any spelling already there. An empty value is not written. */
export function injectTrace(h: MsgHdrs, traceparent: string, tracestate: string): void {
  const c = new HeaderCarrier(h);
  c.set(HEADER_TRACEPARENT, traceparent);
  c.set(HEADER_TRACESTATE, tracestate);
}

/** Reads the trace headers, whatever case they were written in. */
export function extractTrace(h: MsgHdrs | undefined): { traceparent: string; tracestate: string } {
  if (!h) return { traceparent: "", tracestate: "" };
  const c = new HeaderCarrier(h);
  return { traceparent: c.get(HEADER_TRACEPARENT), tracestate: c.get(HEADER_TRACESTATE) };
}
