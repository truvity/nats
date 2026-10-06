package com.truvity.nats

import io.nats.client.impl.Headers

/**
 * The W3C trace-context header names, as written on the wire: lower case.
 *
 * The trap: NATS headers are HTTP-like, and some drivers canonicalise a key (`traceparent`
 * becomes `Traceparent`) while others send it as given. A reader that looks a key up exactly then
 * misses what another language wrote. The contract: write the lower-case name exactly, read
 * case-insensitively. (jnats keeps a key as given, so it needs no canonicalisation workaround;
 * the helpers are what keeps this adapter and the others agreeing.)
 */
const val HEADER_TRACEPARENT = "traceparent"
const val HEADER_TRACESTATE = "tracestate"

/** Writes the trace headers, replacing any spelling already there. An empty value is not written. */
fun injectTrace(h: Headers, traceparent: String?, tracestate: String?) {
    HeaderCarrier(h).set(HEADER_TRACEPARENT, traceparent)
    HeaderCarrier(h).set(HEADER_TRACESTATE, tracestate)
}

/** Reads the trace headers, whatever case they were written in. Missing ones are empty strings. */
fun extractTrace(h: Headers): Pair<String, String> {
    val c = HeaderCarrier(h)
    return c.get(HEADER_TRACEPARENT) to c.get(HEADER_TRACESTATE)
}

/**
 * [Headers] seen through the contract's casing: [get] matches case-insensitively, [set] replaces
 * every spelling with one lower-case entry. Its shape (get, set, keys) fits an OpenTelemetry
 * `TextMapGetter` / `TextMapSetter` without this package depending on OpenTelemetry.
 */
class HeaderCarrier(private val headers: Headers) {
    /** The first value of [key], matched case-insensitively; empty when absent. */
    fun get(key: String): String =
        headers.keySet().firstOrNull { it.equals(key, ignoreCase = true) }
            ?.let { headers.getFirst(it) }
            .orEmpty()

    /** Replaces every spelling of [key] with one lower-case entry. An empty value removes the key. */
    fun set(key: String, value: String?) {
        val spellings = headers.keySet().filter { it.equals(key, ignoreCase = true) }
        if (spellings.isNotEmpty()) headers.remove(*spellings.toTypedArray())
        if (!value.isNullOrEmpty()) headers.put(key.lowercase(), value)
    }

    /** The header names. */
    fun keys(): List<String> = headers.keySet().toList()
}
