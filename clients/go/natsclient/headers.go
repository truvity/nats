package natsclient

import (
	"strings"

	"github.com/nats-io/nats.go"
)

// The W3C trace-context header names, as written on the wire: lower case.
//
// The trap: NATS headers are HTTP-like, and some drivers canonicalise a key
// ("traceparent" becomes "Traceparent") while others send it as given. A
// reader that looks a key up exactly then misses what another language wrote.
// The contract is: write the lower-case name exactly, read case-insensitively.
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// InjectTrace writes the trace headers, replacing any spelling already there.
// An empty value is not written.
func InjectTrace(h nats.Header, traceparent, tracestate string) {
	HeaderCarrier(h).Set(HeaderTraceparent, traceparent)
	HeaderCarrier(h).Set(HeaderTracestate, tracestate)
}

// ExtractTrace reads the trace headers, whatever case they were written in.
func ExtractTrace(h nats.Header) (traceparent, tracestate string) {
	c := HeaderCarrier(h)
	return c.Get(HeaderTraceparent), c.Get(HeaderTracestate)
}

// HeaderCarrier is a nats.Header that satisfies OpenTelemetry's
// propagation.TextMapCarrier (Get, Set, Keys) with the contract's casing, so
// `otel.GetTextMapPropagator().Inject(ctx, natsclient.HeaderCarrier(h))`
// works without this package importing OpenTelemetry.
type HeaderCarrier nats.Header

// Get returns the first value of key, matched case-insensitively.
func (c HeaderCarrier) Get(key string) string {
	for k, v := range c {
		if strings.EqualFold(k, key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

// Set replaces every spelling of key with one lower-case entry. An empty
// value removes the key.
func (c HeaderCarrier) Set(key, value string) {
	for k := range c {
		if strings.EqualFold(k, key) {
			delete(c, k)
		}
	}
	if value != "" {
		c[strings.ToLower(key)] = []string{value}
	}
}

// Keys lists the header names.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
