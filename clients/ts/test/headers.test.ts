import { headers, Match } from "@nats-io/transport-node";
import { describe, expect, it } from "vitest";
import { extractTrace, HeaderCarrier, injectTrace } from "../src/index.js";

describe("trace headers", () => {
  it("writes lower-case keys exactly", () => {
    const h = headers();
    injectTrace(h, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", "k=v");
    expect(h.keys().sort()).toEqual(["traceparent", "tracestate"]);
    expect(h.has("Traceparent", Match.Exact)).toBe(false);
  });

  it("reads whatever case was written", () => {
    const h = headers();
    h.set("Traceparent", "tp", Match.Exact);
    h.set("TRACESTATE", "a=b", Match.Exact);
    expect(extractTrace(h)).toEqual({ traceparent: "tp", tracestate: "a=b" });
    expect(extractTrace(undefined)).toEqual({ traceparent: "", tracestate: "" });
  });

  it("replaces every spelling and drops empty values", () => {
    const h = headers();
    h.set("Traceparent", "old", Match.Exact);
    h.set("Tracestate", "old", Match.Exact);
    injectTrace(h, "new", "");
    expect(new HeaderCarrier(h).keys()).toEqual(["traceparent"]);
    expect(extractTrace(h).traceparent).toBe("new");
  });
});
