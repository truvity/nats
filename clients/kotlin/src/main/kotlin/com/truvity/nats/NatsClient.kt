package com.truvity.nats

import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.ErrorListener
import io.nats.client.JetStream
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.ServerPool
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.DeliverPolicy
import io.nats.client.api.DiscardPolicy
import io.nats.client.api.PublishAck
import io.nats.client.api.ReplayPolicy
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StreamInfo
import io.nats.client.impl.Headers
import io.nats.client.impl.NatsMessage
import io.nats.client.PublishOptions as JsPublishOptions
import io.nats.client.Consumer as JnatsConsumer
import io.nats.client.support.NatsUri
import java.io.IOException
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

/** The adapter's own failure (a drain that did not finish, a health check that failed, ...). */
open class NatsClientException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * A connection attempt failed. [retryable] says whether a later try can fix it (the broker was not
 * reachable, not yet answering the handshake).
 */
class NatsConnectException(message: String, cause: Throwable?, val retryable: Boolean) :
    NatsClientException(message, cause)

/** The health check is bounded to this. */
val HEALTH_TIMEOUT: Duration = Duration.ofSeconds(2)

/**
 * One connection to the broker. It reconnects without limit; the credential files are read again
 * for every reconnect. Blocking API (jnats's own is), usable from Java as well as Kotlin.
 */
class NatsClient private constructor(
    val config: NatsConfig,
    /** The driver's connection, for what the adapter does not wrap (subscriptions, requests). Do not close it; use [close]. */
    val connection: Connection,
    private val pool: ReconnectPool,
    private val logger: System.Logger,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val js: JetStream by lazy { connection.jetStream() }
    private val jsm: JetStreamManagement by lazy { connection.jetStreamManagement() }

    /** The driver's JetStream context. */
    fun jetStream(): JetStream = js

    /** The driver's JetStream management context. */
    fun jetStreamManagement(): JetStreamManagement = jsm

    /**
     * A round trip to the broker through the real path (TLS, authentication), bounded to 2s. It
     * suits a readiness probe. Throws [NatsClientException] when the broker does not answer.
     */
    fun health() {
        val st = connection.status
        if (st != Connection.Status.CONNECTED) throw NatsClientException("nats-client: not connected ($st)")
        try {
            connection.flush(HEALTH_TIMEOUT)
        } catch (e: TimeoutException) {
            throw NatsClientException("nats-client: health: no answer within $HEALTH_TIMEOUT", e)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw NatsClientException("nats-client: health: interrupted", e)
        } catch (e: RuntimeException) {
            throw NatsClientException("nats-client: health: ${e.message}", e)
        }
    }

    /**
     * Drains the connection: subscriptions stop taking new messages, the ones already delivered are
     * handled, pending publishes are flushed, then the connection closes. Returns when that is done
     * or the drain timeout has passed (then the connection is closed anyway and this throws).
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        pool.release()
        if (connection.status == Connection.Status.CLOSED) return
        var failure: NatsClientException? = null
        try {
            val done = connection.drain(config.drainTimeout)
            if (!done.get(config.drainTimeout.toMillis() + 1000, TimeUnit.MILLISECONDS)) {
                failure = NatsClientException("nats-client: drain did not finish within ${config.drainTimeout}")
            }
        } catch (e: TimeoutException) {
            failure = NatsClientException("nats-client: drain did not finish within ${config.drainTimeout}", e)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            failure = NatsClientException("nats-client: drain interrupted", e)
        } catch (e: ExecutionException) {
            failure = NatsClientException("nats-client: drain: ${e.cause?.message}", e)
        } catch (e: IllegalStateException) {
            // already closing or closed
        } finally {
            runCatching { connection.close() }
        }
        if (failure != null) throw failure
    }

    /** Creates the stream or brings an existing one to the spec. */
    fun ensureStream(spec: StreamSpec): StreamInfo {
        require(spec.name.isNotEmpty() && spec.subjects.isNotEmpty()) { "nats-client: a stream needs a name and subjects" }
        spec.subjects.forEach(::validStreamSubject)
        val cfg = StreamConfiguration.builder()
            .name(spec.name)
            .description(spec.description)
            .subjects(spec.subjects)
            .retentionPolicy(RetentionPolicy.Limits)
            .storageType(StorageType.File)
            .discardPolicy(DiscardPolicy.Old)
            .replicas(if (spec.replicas == 0) DEFAULT_STREAM_REPLICAS else spec.replicas)
            .maxAge(spec.maxAge ?: DEFAULT_STREAM_MAX_AGE)
            .duplicateWindow(DEFAULT_DUPLICATE_WINDOW)
            .build()
        try {
            return jsApi {
                try {
                    jsm.updateStream(cfg)
                } catch (e: JetStreamApiException) {
                    if (e.apiErrorCode != STREAM_NOT_FOUND && e.errorCode != 404) throw e
                    jsm.addStream(cfg)
                }
            }
        } catch (e: JetStreamApiException) {
            throw NatsClientException("nats-client: ensure stream ${spec.name}: ${e.message}", e)
        } catch (e: IOException) {
            throw NatsClientException("nats-client: ensure stream ${spec.name}: ${e.message}", e)
        }
    }

    /** Creates the durable consumer or brings an existing one to the spec. */
    fun ensureConsumer(spec: ConsumerSpec): ConsumerInfo {
        require(spec.stream.isNotEmpty() && spec.durable.isNotEmpty()) { "nats-client: a consumer needs a stream and a durable name" }
        val b = ConsumerConfiguration.builder()
            .durable(spec.durable)
            .ackPolicy(AckPolicy.Explicit)
            .ackWait(spec.ackWait ?: DEFAULT_ACK_WAIT)
            .maxDeliver((if (spec.maxDeliver == 0) DEFAULT_MAX_DELIVER else spec.maxDeliver).toLong())
            .maxAckPending((if (spec.maxAckPending == 0) DEFAULT_MAX_ACK_PENDING else spec.maxAckPending).toLong())
            .deliverPolicy(DeliverPolicy.All)
            .replayPolicy(ReplayPolicy.Instant)
        if (spec.filterSubject.isNotEmpty()) b.filterSubject(spec.filterSubject)
        try {
            return jsApi { jsm.addOrUpdateConsumer(spec.stream, b.build()) }
        } catch (e: JetStreamApiException) {
            throw NatsClientException("nats-client: ensure consumer ${spec.stream}/${spec.durable}: ${e.message}", e)
        } catch (e: IOException) {
            throw NatsClientException("nats-client: ensure consumer ${spec.stream}/${spec.durable}: ${e.message}", e)
        }
    }

    /**
     * Sends [data] to a JetStream subject and waits for the stream's acknowledgement, retrying a
     * missing responder (a stream still being created, a leader election) a few times. The subject
     * is checked first ([validPublishSubject]); the trace headers are written lower-case.
     */
    @JvmOverloads
    fun publish(subject: String, data: ByteArray, options: PublishOptions = PublishOptions()): PublishAck {
        validPublishSubject(subject)
        val headers = Headers()
        options.headers?.forEach { k, v -> headers.add(k, v) }
        injectTrace(headers, options.traceparent, options.tracestate)
        val msg = NatsMessage.builder().subject(subject).data(data).headers(headers).build()
        val po = JsPublishOptions.builder().apply { if (options.msgId.isNotEmpty()) messageId(options.msgId) }.build()
        val timeout = options.timeout ?: DEFAULT_PUBLISH_TIMEOUT
        var last: Throwable? = null
        for (attempt in 1..PUBLISH_RETRY_ATTEMPTS) {
            try {
                return js.publishAsync(msg, po).get(timeout.toNanos(), TimeUnit.NANOSECONDS)
            } catch (e: ExecutionException) {
                last = e.cause ?: e
                if (!noResponders(last) || attempt == PUBLISH_RETRY_ATTEMPTS) break
            } catch (e: TimeoutException) {
                throw NatsClientException("nats-client: publish $subject: no acknowledgement within $timeout", e)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw NatsClientException("nats-client: publish $subject: interrupted", e)
            }
            try {
                Thread.sleep(100L * attempt)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw NatsClientException("nats-client: publish $subject: interrupted", e)
            }
        }
        throw NatsClientException("nats-client: publish $subject: ${last?.message}", last)
    }

    /**
     * A JetStream API call, retried while JetStream is not answering yet (no responders, a timeout:
     * a broker just restarted, a leader election).
     */
    private fun <T> jsApi(call: () -> T): T =
        retry(JS_API_RETRY, { it is IOException && (noResponders(it) || "timeout" in it.message.orEmpty().lowercase()) }, call)

    companion object {
        private val JS_API_RETRY = RetryPolicy(6, Duration.ofMillis(250), Duration.ofSeconds(2), Duration.ofSeconds(15))
        const val DEFAULT_STREAM_REPLICAS = 1
        val DEFAULT_STREAM_MAX_AGE: Duration = Duration.ofDays(7)
        val DEFAULT_DUPLICATE_WINDOW: Duration = Duration.ofMinutes(2)
        val DEFAULT_ACK_WAIT: Duration = Duration.ofSeconds(30)
        const val DEFAULT_MAX_DELIVER = 5
        const val DEFAULT_MAX_ACK_PENDING = 1000
        private const val PUBLISH_RETRY_ATTEMPTS = 3
        private val DEFAULT_PUBLISH_TIMEOUT: Duration = Duration.ofSeconds(5)
        private const val STREAM_NOT_FOUND = 10059

        private fun noResponders(t: Throwable): Boolean {
            val m = generateSequence(t) { it.cause }.mapNotNull { it.message?.lowercase() }.joinToString(" ")
            return "no responders" in m || "503" in m
        }

        /**
         * Validates [config] and opens the connection. The first connection is retried (see
         * [NatsConfig.connect]) while the broker is not reachable; a failed verification or
         * authorization is thrown at once. Blocking; interrupting the thread stops the retry.
         */
        @JvmStatic
        @JvmOverloads
        fun connect(config: NatsConfig, logger: System.Logger = System.getLogger("com.truvity.nats")): NatsClient {
            config.validate()
            val uri = URI(config.url)
            val ssl = if (config.tls) {
                try {
                    ReloadingSslContext.create(config.caFile!!, config.certFile, config.keyFile, uri.host)
                } catch (e: Exception) {
                    throw NatsTlsException("nats-client: TLS files unusable: ${e.message}", e)
                }
            } else {
                null
            }
            return try {
                retry(config.connect) { connectOnce(config, ssl, logger) }
            } catch (e: NatsConnectException) {
                throw NatsConnectException("nats-client: connect ${redactUrl(config.url)}: ${e.message}", e.cause, e.retryable)
            }
        }

        private fun connectOnce(config: NatsConfig, ssl: ReloadingSslContext?, logger: System.Logger): NatsClient {
            val listener = Listener(logger)
            val pool = ReconnectPool(config.url, config.reconnectWait)
            val b = Options.Builder()
                .serverPool(pool)
                .hostnameResolveMode(Options.HostnameResolveMode.HappyEyeballs)
                .ignoreDiscoveredServers()
                .connectionName(config.name)
                .connectionTimeout(config.connectTimeout)
                .pingInterval(config.pingInterval)
                .maxReconnects(-1)
                .reconnectWait(config.reconnectWait)
                .connectionListener(listener)
                .errorListener(listener)
            if (ssl != null) b.sslContext(ssl)
            config.tokenFile?.takeIf { it.isNotEmpty() }?.let { f ->
                // Read for every new physical connection: jnats asks the supplier each time it
                // builds the CONNECT message, on the first connection and on every reconnect.
                b.tokenSupplier(FileTokenSupplier(f, logger))
            }
            ssl?.lastVerifyFailure?.set(null)
            fun failed(e: IOException?): Nothing {
                val verify = ssl?.lastVerifyFailure?.get()
                if (verify != null) {
                    throw NatsTlsException("certificate verification failed: ${verify.message}", verify)
                }
                val refusal = listener.lastError
                if (refusal != null && Regex("authori[sz]ation|authentication|permissions", RegexOption.IGNORE_CASE).containsMatchIn(refusal)) {
                    throw NatsConnectException(refusal, e, false)
                }
                val seen = listener.lastException
                if (seen is javax.net.ssl.SSLException) {
                    throw NatsTlsException("TLS handshake failed (a refused certificate?): ${seen.message}", seen)
                }
                // TLS 1.3 reports the server's refusal of a client certificate only after the client's
                // handshake is over, and jnats then waits out the connect timeout without reporting it.
                // A certificate identity bypasses the callout, so the broker answers at once: a
                // handshake that got as far as sending the certificate and then got no answer means
                // the certificate was refused.
                if (ssl != null && ssl.presentsClientCertificate && ssl.certificateSent) {
                    throw NatsTlsException(
                        "the broker did not accept the connection after the client certificate was sent: " +
                            "it most likely refused the client certificate (not issued by the broker's client CA?)",
                        e,
                    )
                }
                val cause: Throwable = seen ?: e ?: IOException("the connection was refused")
                throw NatsConnectException(
                    e?.message ?: cause.message ?: cause.toString(), cause,
                    (e == null || isRetryable(e)) && (seen == null || isRetryable(seen)),
                )
            }

            val conn = try {
                Nats.connect(b.build())
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw NatsConnectException("interrupted", e, false)
            } catch (e: IOException) {
                failed(e)
            }
            // Closed by the fail-fast listener: jnats returns the dead connection instead of throwing.
            if (conn.status == Connection.Status.CLOSED) failed(null)
            listener.failFast = false
            return NatsClient(config, conn, pool, logger)
        }
    }
}

/** Names a stream; zero fields take the defaults (file storage, limits retention, discard old, 1 replica, 7 days, 2 minute de-duplication window). */
data class StreamSpec(
    val name: String,
    val subjects: List<String>,
    val description: String = "",
    /** 0 means the default (1); run 3 against a three-node broker. */
    val replicas: Int = 0,
    val maxAge: Duration? = null,
)

/** Names a durable consumer; zero fields take the defaults (explicit ack, 30s ack wait, 5 deliveries, 1000 unacknowledged, deliver all). */
data class ConsumerSpec(
    val stream: String,
    val durable: String,
    val filterSubject: String = "",
    val ackWait: Duration? = null,
    val maxDeliver: Int = 0,
    val maxAckPending: Int = 0,
)

/** What a publish adds to the subject and payload. */
data class PublishOptions(
    /** Makes a retried publish idempotent within the stream's de-duplication window. */
    val msgId: String = "",
    /** Written as the lower-case trace headers. */
    val traceparent: String? = null,
    val tracestate: String? = null,
    /** Extra headers, sent as given. */
    val headers: Map<String, List<String>>? = null,
    /** How long to wait for the acknowledgement; default 5s. */
    val timeout: Duration? = null,
)

/** Reads the token file every time it is asked; an unreadable file yields no token (and a log line). */
private class FileTokenSupplier(private val file: String, private val logger: System.Logger) : Supplier<CharArray?> {
    override fun get(): CharArray? =
        try {
            Files.readString(Path.of(file)).trim().toCharArray()
        } catch (e: IOException) {
            logger.log(System.Logger.Level.ERROR, "nats token file unreadable: $file: ${e.message}")
            null
        }
}

/** Logs what the driver reports (nothing it is given contains a credential) and remembers the last failure. */
private class Listener(private val logger: System.Logger) : ConnectionListener, ErrorListener {
    @Volatile
    var lastException: Exception? = null

    /**
     * True while the first connection is being made. jnats keeps waiting for the broker's PONG until
     * the connect timeout after the broker refused the handshake (an authorization error, a refused
     * client certificate), so a refusal would only be reported after 5s. During the first connection
     * a refusal closes the connection at once, which makes `Nats.connect` fail now and by name. After
     * it the driver is left alone to reconnect.
     */
    @Volatile
    var failFast = true

    /** The last error text the broker sent (`-ERR`), e.g. "Authorization Violation". */
    @Volatile
    var lastError: String? = null

    private fun refused() {
        val conn = connectionRef ?: return
        if (failFast) Thread { runCatching { conn.close() } }.apply { isDaemon = true }.start()
    }

    @Volatile
    var connectionRef: Connection? = null

    @Suppress("OVERRIDE_DEPRECATION")
    override fun connectionEvent(conn: Connection, type: ConnectionListener.Events) {
        val level = if (type == ConnectionListener.Events.DISCONNECTED) System.Logger.Level.WARNING else System.Logger.Level.INFO
        logger.log(level, "nats {0}", type.event)
    }

    override fun errorOccurred(conn: Connection, error: String?) {
        logger.log(System.Logger.Level.ERROR, "nats error: {0}", error)
        lastError = error
        connectionRef = conn
        refused()
    }

    override fun exceptionOccurred(conn: Connection, exp: Exception) {
        lastException = exp
        logger.log(System.Logger.Level.ERROR, "nats error: {0}", exp.toString())
        connectionRef = conn
        refused()
    }

    override fun slowConsumerDetected(conn: Connection, consumer: JnatsConsumer) {
        logger.log(System.Logger.Level.WARNING, "nats slow consumer")
    }
}

/**
 * The one broker URL as a server pool.
 *
 * jnats gives up reconnecting after two identical authorization errors from the same server
 * (`NatsConnection.reconnectImplConnect`, "double auth error"; nats.go's `IgnoreAuthErrorAbort`
 * has no jnats counterpart). That must not happen here: a broker that restarts can answer before
 * its callout is back, and a rotated token is read only by the next attempt. jnats keys that
 * comparison by server URI, so after the first connection this pool hands out the same URL with a
 * different fragment each time, which makes every attempt a "different" server; the fragment is
 * never part of what is dialled. It also paces the attempts itself (reconnect wait plus jitter),
 * because jnats paces a round only when it sees the same server come round again.
 */
private class ReconnectPool(private val url: String, private val wait: Duration) : ServerPool {
    private val counter = AtomicInteger()
    private val connectedOnce = AtomicBoolean(false)
    private val failedSinceSuccess = AtomicBoolean(false)
    private val released = CountDownLatch(1)

    fun release() = released.countDown()

    override fun initialize(opts: Options) = Unit
    override fun acceptDiscoveredUrls(discoveredServers: List<String>): Boolean = false

    // During the first connection jnats peeks, takes, and stops when it sees the same server again,
    // so there the URI must be stable.
    override fun peekNextServer(): NatsUri = if (connectedOnce.get()) next() else NatsUri(url)

    override fun nextServer(): NatsUri? {
        if (!connectedOnce.get()) return NatsUri(url)
        if (failedSinceSuccess.get()) {
            val jitter = ThreadLocalRandom.current().nextLong(wait.toMillis() / 2 + 1)
            if (released.await(wait.toMillis() + jitter, TimeUnit.MILLISECONDS)) return null
        }
        return next()
    }

    private fun next(): NatsUri = NatsUri("$url#${counter.incrementAndGet()}")

    @Suppress("OVERRIDE_DEPRECATION")
    override fun resolveHostToIps(host: String): List<String>? = null

    override fun connectSucceeded(nuri: NatsUri) {
        connectedOnce.set(true)
        failedSinceSuccess.set(false)
    }

    override fun connectFailed(nuri: NatsUri) {
        failedSinceSuccess.set(true)
    }

    override fun getServerList(): List<String> = listOf(redactUrl(url))
    override fun hasSecureServer(): Boolean = url.startsWith("tls://")
}
