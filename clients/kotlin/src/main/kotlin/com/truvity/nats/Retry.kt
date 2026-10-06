package com.truvity.nats

import io.nats.client.AuthenticationException
import java.io.IOException
import java.net.ConnectException
import java.time.Duration
import java.util.concurrent.ThreadLocalRandom
import javax.net.ssl.SSLException

/**
 * Bounds the retry of connection-class failures: exponential backoff with full jitter, stopping
 * at [attempts] or [budget], whichever comes first.
 */
data class RetryPolicy(
    /** Total tries, including the first. */
    val attempts: Int = 5,
    /** Ceiling of the first sleep. */
    val initialDelay: Duration = Duration.ofMillis(200),
    /** Ceiling of any sleep. */
    val maxDelay: Duration = Duration.ofSeconds(5),
    /** Total time that may be spent waiting. */
    val budget: Duration = Duration.ofSeconds(30),
) {
    /** The problem with this policy, or null when it is valid. */
    internal fun problem(): String? =
        if (attempts < 1 || initialDelay.isZero || initialDelay.isNegative || maxDelay < initialDelay ||
            budget.isZero || budget.isNegative
        ) {
            "retry policy $this: attempts>=1, 0<initialDelay<=maxDelay, budget>0"
        } else {
            null
        }

    companion object {
        /** 5 tries, 200ms doubling to 5s, 30s of waiting. */
        @JvmStatic
        fun default(): RetryPolicy = RetryPolicy()
    }
}

/**
 * Runs [fn], repeating it while it fails with a retryable error (see [isRetryable]). [fn] must be
 * safe to run again. Blocking: the calling thread sleeps between tries, and interrupting it stops
 * the retry (the interrupt flag stays set and the last error is rethrown).
 */
fun <T> retry(policy: RetryPolicy, isRetryable: (Throwable) -> Boolean = ::isRetryable, fn: () -> T): T {
    policy.problem()?.let { throw ConfigError(listOf(it)) }
    val start = System.nanoTime()
    var attempt = 1
    while (true) {
        try {
            return fn()
        } catch (e: Exception) {
            if (Thread.currentThread().isInterrupted || !isRetryable(e) || attempt >= policy.attempts) throw e
            val delay = backoff(policy, attempt)
            if (Duration.ofNanos(System.nanoTime() - start) + delay > policy.budget) throw e
            try {
                Thread.sleep(delay.toMillis())
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            }
        }
        attempt++
    }
}

internal fun backoff(p: RetryPolicy, attempt: Int): Duration {
    var ceil = p.initialDelay
    var i = 1
    while (i < attempt && ceil < p.maxDelay) {
        ceil = ceil.multipliedBy(2)
        i++
    }
    if (ceil > p.maxDelay) ceil = p.maxDelay
    return Duration.ofNanos(ThreadLocalRandom.current().nextLong(ceil.toNanos() + 1))
}

/**
 * Whether [t] means the broker was not reachable (down, restarting, not accepting yet), which a
 * later try can fix. The whole cause chain is read.
 *
 * Never retryable: an authorization or authentication failure, a certificate verification or
 * other TLS failure (a second try cannot fix either), a permissions violation, and the caller's
 * own interruption.
 */
fun isRetryable(t: Throwable?): Boolean {
    if (t == null) return false
    var connectionClass = false
    var cur: Throwable? = t
    var depth = 0
    val seen = HashSet<Throwable>()
    while (cur != null && seen.add(cur) && depth++ < 16) {
        when (cur) {
            is InterruptedException, is java.util.concurrent.CancellationException -> return false
            is AuthenticationException, is NatsTlsException, is SSLException,
            is java.security.cert.CertificateException, is ConfigError -> return false
            is NatsConnectException -> connectionClass = connectionClass || cur.retryable
            is ConnectException, is java.net.SocketTimeoutException, is java.io.EOFException,
            is java.net.SocketException, is java.util.concurrent.TimeoutException -> connectionClass = true
            is IOException -> connectionClass = true
        }
        val msg = cur.message?.lowercase().orEmpty()
        if ("authorization violation" in msg || "authentication" in msg || "certificate" in msg ||
            "permissions violation" in msg || "ssl connection wanted" in msg || "ssl required" in msg
        ) {
            return false
        }
        // The broker answered the handshake with a PING while it was still asking its callout: it is
        // up and busy, not refusing.
        if ("expected 'pong'" in msg) connectionClass = true
        cur = cur.cause
    }
    return connectionClass
}
