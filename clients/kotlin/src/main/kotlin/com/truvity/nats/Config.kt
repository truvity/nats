package com.truvity.nats

import java.time.Duration

/** Configuration the adapter refuses; [problems] lists every one at once. */
class ConfigError(val problems: List<String>) : RuntimeException(problems.joinToString("\n") { "nats-client: $it" })

/**
 * Everything a connection needs. Start from the defaults and override; a [NatsConfig] with no URL
 * and no credential is invalid.
 *
 * There is deliberately no switch that turns verification off and no free-form option string: a
 * parameter handed through a string can be dropped on the way to the driver, and a dropped CA
 * turns a verified connection into one that does not verify.
 */
data class NatsConfig(
    /** The broker, one nats:// or tls:// URL. The server certificate must carry its host name. */
    val url: String = "",
    /** The server CA. When set the connection is TLS and trusts this file and nothing else. */
    val caFile: String? = null,
    /** The workload certificate (a SPIFFE identity); with [keyFile], both or neither. Re-read for every connection. */
    val certFile: String? = null,
    val keyFile: String? = null,
    /** The projected ServiceAccount token, sent as the NATS auth token. Re-read for every connection. */
    val tokenFile: String? = null,
    /** The connection name the broker shows. Default: the program name. */
    val name: String = programName(),
    val connectTimeout: Duration = Duration.ofSeconds(5),
    val reconnectWait: Duration = Duration.ofSeconds(1),
    val pingInterval: Duration = Duration.ofSeconds(30),
    val drainTimeout: Duration = Duration.ofSeconds(10),
    /** Bounds the retry of the first connection; afterwards the driver reconnects without limit. */
    val connect: RetryPolicy = RetryPolicy(),
) {
    internal val tls: Boolean get() = !caFile.isNullOrEmpty()

    /** Throws [ConfigError] listing every problem, not just the first. */
    fun validate() {
        val errs = mutableListOf<String>()
        var scheme = ""
        if (url.isEmpty()) {
            errs += "URL is required (NATS_URL)"
        } else {
            if (url.any { it == ',' || it == ' ' }) {
                errs += "URL \"${redactUrl(url)}\": exactly one URL is accepted (the broker's Service)"
            }
            val u = runCatching { java.net.URI(url) }.getOrNull()
            when {
                u == null || u.host.isNullOrEmpty() -> errs += "URL is not a nats:// or tls:// URL with a host"
                u.scheme != "nats" && u.scheme != "tls" ->
                    errs += "URL scheme \"${u.scheme}\" is refused; only nats:// and tls:// are accepted"
                else -> scheme = u.scheme
            }
            if (USERINFO.containsMatchIn(url)) {
                errs += "credentials in the URL are refused; use the token file or the certificate"
            }
        }
        if (scheme == "tls" && caFile.isNullOrEmpty()) {
            errs += "a tls:// URL needs the server CA file (NATS_CA_FILE): verification has nothing to verify against without it"
        }
        if (certFile.isNullOrEmpty() != keyFile.isNullOrEmpty()) {
            errs += "client certificate and key go together"
        }
        val cert = !certFile.isNullOrEmpty() || !keyFile.isNullOrEmpty()
        val token = !tokenFile.isNullOrEmpty()
        when {
            cert && token -> errs += "a certificate and a token file are two identities; give one"
            !cert && !token ->
                errs += "a credential is required: a certificate and key (NATS_CERT_FILE, NATS_KEY_FILE) or a token file (NATS_TOKEN_FILE)"
        }
        if (!certFile.isNullOrEmpty() && !keyFile.isNullOrEmpty() && caFile.isNullOrEmpty()) {
            errs += "a client certificate needs the server CA file: it is only ever sent over a verified connection"
        }
        if (listOf(connectTimeout, reconnectWait, pingInterval, drainTimeout).any { it.isZero || it.isNegative }) {
            errs += "connectTimeout, reconnectWait, pingInterval and drainTimeout must be positive"
        }
        connect.problem()?.let { errs += it }
        if (errs.isNotEmpty()) throw ConfigError(errs)
    }

    /** Shows no credential, only which files are in use. */
    override fun toString(): String =
        "NatsConfig(url=${redactUrl(url)} ca=$caFile cert=$certFile key=$keyFile token_file=$tokenFile name=$name)"

    companion object {
        /**
         * Builds a validated [NatsConfig] from the contract's environment (`NATS_URL`, `NATS_CA_FILE`,
         * ...), starting from the defaults. Pass `System::getenv` or a map lookup in tests.
         */
        @JvmStatic
        @JvmOverloads
        fun fromEnv(getenv: (String) -> String? = System::getenv): NatsConfig {
            val errs = mutableListOf<String>()
            fun str(n: String): String? = getenv(n)?.takeIf { it.isNotEmpty() }
            fun dur(n: String, dflt: Duration): Duration {
                val v = str(n) ?: return dflt
                return parseDuration(v) ?: dflt.also { errs += "$n=\"$v\": not a duration (5s, 500ms, 1m30s)" }
            }
            val d = NatsConfig()
            var attempts = d.connect.attempts
            str("NATS_CLIENT_RETRY_ATTEMPTS")?.let { v ->
                v.toIntOrNull()?.let { attempts = it } ?: errs.add("NATS_CLIENT_RETRY_ATTEMPTS=\"$v\": not an integer")
            }
            val c = NatsConfig(
                url = str("NATS_URL") ?: "",
                caFile = str("NATS_CA_FILE"),
                certFile = str("NATS_CERT_FILE"),
                keyFile = str("NATS_KEY_FILE"),
                tokenFile = str("NATS_TOKEN_FILE"),
                name = str("NATS_CLIENT_NAME") ?: d.name,
                connectTimeout = dur("NATS_CLIENT_CONNECT_TIMEOUT", d.connectTimeout),
                reconnectWait = dur("NATS_CLIENT_RECONNECT_WAIT", d.reconnectWait),
                pingInterval = dur("NATS_CLIENT_PING_INTERVAL", d.pingInterval),
                drainTimeout = dur("NATS_CLIENT_DRAIN_TIMEOUT", d.drainTimeout),
                connect = RetryPolicy(
                    attempts = attempts,
                    initialDelay = d.connect.initialDelay,
                    maxDelay = dur("NATS_CLIENT_RETRY_MAX_DELAY", d.connect.maxDelay),
                    budget = dur("NATS_CLIENT_RETRY_BUDGET", d.connect.budget),
                ),
            )
            if (errs.isNotEmpty()) throw ConfigError(errs)
            c.validate()
            return c
        }
    }
}

private val USERINFO = Regex("^[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*@")

/** [url] without any user info (`nats://user:pw@host` becomes `nats://host`). */
fun redactUrl(url: String): String = url.replace(Regex("(?<=://)[^/?#]*@"), "")

internal fun programName(): String {
    val cmd = System.getProperty("sun.java.command")?.trim()?.substringBefore(' ').orEmpty()
    val base = cmd.substringAfterLast('/').substringAfterLast('\\')
    return base.ifEmpty { "kotlin" }
}

private val DURATION_PART = Regex("([0-9]+(?:\\.[0-9]+)?)(ns|us|µs|ms|s|m|h)")

/** Parses a Go-style duration (`500ms`, `5s`, `1m30s`); null when it is not one. */
fun parseDuration(s: String): Duration? {
    if (s.isEmpty() || s == "0") return if (s == "0") Duration.ZERO else null
    var pos = 0
    var nanos = 0.0
    while (pos < s.length) {
        val m = DURATION_PART.matchAt(s, pos) ?: return null
        val unit = when (m.groupValues[2]) {
            "ns" -> 1.0
            "us", "µs" -> 1e3
            "ms" -> 1e6
            "s" -> 1e9
            "m" -> 6e10
            else -> 3.6e12
        }
        nanos += m.groupValues[1].toDouble() * unit
        pos = m.range.last + 1
    }
    return Duration.ofNanos(nanos.toLong())
}
