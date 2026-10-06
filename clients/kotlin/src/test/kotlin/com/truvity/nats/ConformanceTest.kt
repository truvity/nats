package com.truvity.nats

import com.fasterxml.jackson.databind.ObjectMapper
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.AckPolicy
import io.nats.client.api.DeliverPolicy
import io.nats.client.api.DiscardPolicy
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StorageType
import io.nats.client.impl.Headers
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.fail
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * The conformance suite (clients/conformance/cases.txt): each case is one test whose method name
 * is exactly the case name, run against the broker that clients/conformance/nats-broker.sh starts.
 * Without NATS_CLIENTS_URL it is skipped, unless NATS_CLIENTS=required, which turns a missing
 * broker into a failure. clients/conformance/guard.sh fails unless every case ran and passed.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConformanceTest {
    private val env = System.getenv()
    private val url = env["NATS_CLIENTS_URL"].orEmpty()
    private val tlsUrl = env["NATS_CLIENTS_TLS_URL"].orEmpty()
    private val tlsUrlIp = env["NATS_CLIENTS_TLS_URL_IP"].orEmpty()
    private val monitor = env["NATS_CLIENTS_MONITOR"].orEmpty()
    private val dir = env["NATS_CLIENTS_DIR"].orEmpty()
    private val container = env["NATS_CLIENTS_CONTAINER"].orEmpty()
    private val trust = env["NATS_CLIENTS_TRUST_DOMAIN"].orEmpty()

    // Subjects and streams are unique per run: a broker may be reused.
    private val run = "r${System.nanoTime()}"
    private fun subj(s: String) = "conformance.$run.$s"
    private val apiId get() = "spiffe://$trust/ns/shop/sa/api"
    private val workerId get() = "spiffe://$trust/ns/shop/sa/worker"

    private val clients = CopyOnWriteArrayList<NatsClient>()
    private val http = HttpClient.newHttpClient()
    private val mapper = ObjectMapper()

    private val certs = listOf(
        "ca.crt", "other-ca.crt", "api.crt", "api.key", "worker.crt", "worker.key", "foreign.crt", "foreign.key",
    )

    @BeforeAll
    fun requireBroker() {
        if (url.isEmpty()) {
            if (env["NATS_CLIENTS"] == "required") fail("NATS_CLIENTS=required but NATS_CLIENTS_URL is not set: no broker")
            assumeTrue(false, "NATS_CLIENTS_URL not set: no broker")
        }
    }

    @AfterEach
    fun closeClients() {
        clients.forEach { runCatching { it.close() } }
        clients.clear()
    }

    /** A per-test directory holding copies of [names], so a test can rotate them. */
    private fun work(names: List<String> = emptyList()): Path {
        val d = Files.createTempDirectory("nats-kt-")
        for (n in names) Files.copy(Path.of(dir, n), d.resolve(n))
        return d
    }

    private fun certConfig(name: String, d: Path, cert: String) = NatsConfig(
        url = tlsUrl, caFile = d.resolve("ca.crt").toString(), certFile = d.resolve("$cert.crt").toString(),
        keyFile = d.resolve("$cert.key").toString(), name = name, reconnectWait = Duration.ofMillis(200),
    )

    private fun tokenConfig(name: String, tokenFile: Path) =
        NatsConfig(url = url, tokenFile = tokenFile.toString(), name = name, reconnectWait = Duration.ofMillis(200))

    private fun tokenFile(d: Path, token: String): Path = d.resolve("token").also { Files.writeString(it, token) }

    private fun connect(cfg: NatsConfig): NatsClient = NatsClient.connect(cfg).also { clients += it }

    /** The connection is refused, quickly: a verification or authorization failure is not retried. */
    private fun connectFails(cfg: NatsConfig): Throwable {
        val c = cfg.copy(connect = cfg.connect.copy(budget = Duration.ofSeconds(2), attempts = 3))
        val start = System.nanoTime()
        val t = try {
            clients += NatsClient.connect(c)
            fail("the connection was accepted")
            throw AssertionError()
        } catch (e: NatsClientException) {
            e
        } catch (e: NatsTlsException) {
            e
        } catch (e: io.nats.client.AuthenticationException) {
            e
        }
        assertTrue(Duration.ofNanos(System.nanoTime() - start) < Duration.ofSeconds(5), "failed slowly (retried?): $t")
        return t
    }

    private fun eventually(timeout: Duration, interval: Duration = Duration.ofMillis(100), what: () -> String = { "condition" }, cond: () -> Boolean) {
        val end = System.nanoTime() + timeout.toNanos()
        while (true) {
            if (runCatching(cond).getOrDefault(false)) return
            if (System.nanoTime() > end) fail("timed out waiting for ${what()}")
            Thread.sleep(interval.toMillis())
        }
    }

    private data class ConnInfo(val name: String, val account: String, val authorizedUser: String)

    /** Who the broker's monitor says is connected, with the identity it authorized. */
    private fun connz(): List<ConnInfo> {
        val r = http.send(HttpRequest.newBuilder(URI("$monitor/connz?auth=true&state=open")).build(), HttpResponse.BodyHandlers.ofString())
        return (mapper.readTree(r.body())["connections"] ?: mapper.createArrayNode()).map {
            ConnInfo(it["name"]?.asText().orEmpty(), it["account"]?.asText().orEmpty(), it["authorized_user"]?.asText().orEmpty())
        }
    }

    private fun waitConn(name: String, ok: (ConnInfo) -> Boolean = { true }): ConnInfo {
        var last: ConnInfo? = null
        eventually(Duration.ofSeconds(30), what = { "connection \"$name\": last seen $last" }) {
            last = connz().firstOrNull { it.name == name }
            last != null && ok(last!!)
        }
        return last!!
    }

    private fun restart() {
        val p = ProcessBuilder("docker", "restart", "-t", "1", container).redirectErrorStream(true).start()
        val out = p.inputStream.readAllBytes().decodeToString()
        assertEquals(0, p.waitFor(), out)
        eventually(Duration.ofSeconds(30), what = { "the broker to come back" }) {
            http.send(HttpRequest.newBuilder(URI("$monitor/healthz")).build(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200
        }
        // Up is not ready: the callout reconnects a moment later, and until it does the broker
        // refuses every token. Wait until one is let in.
        eventually(Duration.ofSeconds(30), Duration.ofMillis(200), what = { "the callout to come back" }) {
            Nats.connect(Options.Builder().server(url).token("token-shop-api".toCharArray()).connectionTimeout(Duration.ofSeconds(2)).build()).use { true }
        }
    }

    private fun Connection.use(f: () -> Boolean): Boolean = try { f() } finally { close() }

    // --- the cases -------------------------------------------------------------------------------

    @Test
    fun `verify-full-connects`() {
        val d = work(certs)
        val c = connect(certConfig("verify-full-connects", d, "api"))
        c.health()
        val got = waitConn("verify-full-connects")
        assertEquals(apiId, got.authorizedUser)
        assertEquals("shop", got.account)
    }

    @Test
    fun `rejects-unknown-ca`() {
        val d = work(certs)
        val cfg = certConfig("rejects-unknown-ca", d, "api").copy(caFile = d.resolve("other-ca.crt").toString())
        assertTrue("certificate" in connectFails(cfg).message!!.lowercase())
    }

    @Test
    fun `rejects-hostname-mismatch`() {
        val d = work(certs)
        // The server certificate carries only `localhost`.
        val cfg = certConfig("rejects-hostname-mismatch", d, "api").copy(url = tlsUrlIp)
        assertTrue("certificate" in connectFails(cfg).message!!.lowercase())
    }

    @Test
    fun `rejects-foreign-client-cert`() {
        val d = work(certs)
        // jnats cannot see a refused client certificate before the connect timeout (TLS 1.3 reports it
        // after the handshake), so this case runs with a short one; the failure is still not retried.
        val cfg = certConfig("rejects-foreign-client-cert", d, "foreign").copy(connectTimeout = Duration.ofSeconds(2))
        val err = connectFails(cfg)
        assertTrue("certificate" in err.message!!.lowercase(), err.toString())
    }

    @Test
    fun `refuses-unsafe-config`() {
        // No CA, a certificate without a key, and a second identity.
        val e = assertThrows(ConfigError::class.java) {
            NatsConfig(url = "tls://localhost:4222", certFile = "/x.crt", tokenFile = "/token").validate()
        }
        // Every problem at once, not the first.
        for (want in listOf("server CA", "go together", "two identities")) assertTrue(want in e.message!!, want)
        assertTrue("scheme" in assertThrows(ConfigError::class.java) {
            NatsConfig(url = "http://localhost:4222", tokenFile = "/token").validate()
        }.message!!)
        val withCreds = assertThrows(ConfigError::class.java) {
            NatsConfig(url = "nats://user:pw@localhost:4222", tokenFile = "/token").validate()
        }
        assertTrue("credentials in the URL" in withCreds.message!!)
        assertFalse("pw" in withCreds.message!!)
        // A broker that does not offer TLS is not talked to in the clear when a CA was given.
        val d = work(certs)
        val plain = tokenConfig("refuses-unsafe-config", tokenFile(d, "token-shop-api")).copy(caFile = d.resolve("ca.crt").toString())
        connectFails(plain)
    }

    @Test
    fun `token-connects`() {
        val d = work()
        val c = connect(tokenConfig("token-connects", tokenFile(d, "token-shop-api\n")))
        c.health()
        assertEquals("shop", waitConn("token-connects").account)
    }

    @Test
    fun `token-account-mapping`() {
        val d = work()
        connect(tokenConfig("token-account-mapping", tokenFile(d, "token-other-app")))
        val got = waitConn("token-account-mapping")
        assertEquals(accountForNamespace("other", listOf("shop", "other")), got.account)
    }

    @Test
    fun `rejects-unmapped-namespace`() {
        val d = work()
        connectFails(tokenConfig("rejects-unmapped-namespace", tokenFile(d, "token-kube-system")))
        val unknown = d.resolve("unknown").also { Files.writeString(it, "not-a-token") }
        connectFails(tokenConfig("rejects-unknown-token", unknown))
    }

    @Test
    fun `client-cert-rotation`() {
        val d = work(certs)
        val c = connect(certConfig("client-cert-rotation", d, "api"))
        assertEquals(apiId, waitConn("client-cert-rotation").authorizedUser)
        // The files change under the running process, as cert-manager renews them.
        for (ext in listOf("crt", "key")) {
            Files.write(d.resolve("api.$ext"), Files.readAllBytes(Path.of(dir, "worker.$ext")))
        }
        restart()
        assertEquals(workerId, waitConn("client-cert-rotation") { it.authorizedUser == workerId }.authorizedUser)
        eventually(Duration.ofSeconds(20)) { c.health(); true }
    }

    @Test
    fun `token-file-rotation`() {
        val d = work()
        val file = tokenFile(d, "token-shop-api")
        val c = connect(tokenConfig("token-file-rotation", file))
        val first = waitConn("token-file-rotation")
        Files.writeString(file, "token-shop-worker")
        restart()
        val got = waitConn("token-file-rotation") {
            it.account == "shop" && it.authorizedUser.isNotEmpty() && it.authorizedUser != first.authorizedUser
        }
        assertNotEquals(first.authorizedUser, got.authorizedUser)
        assertEquals("shop", got.account)
        eventually(Duration.ofSeconds(20)) { c.health(); true }
    }

    @Test
    fun `reconnects-after-broker-restart`() {
        val d = work()
        val c = connect(tokenConfig("reconnects-after-broker-restart", tokenFile(d, "token-shop-api")))
        val got = AtomicInteger()
        val subject = subj("restart")
        c.connection.createDispatcher { got.incrementAndGet() }.subscribe(subject)
        c.connection.flush(Duration.ofSeconds(5))
        restart()
        // The subscription is restored by the driver; a publish after the reconnect reaches it
        // without the caller doing anything.
        eventually(Duration.ofSeconds(30), Duration.ofMillis(200)) {
            c.health()
            c.connection.publish(subject, "x".toByteArray())
            c.connection.flush(Duration.ofSeconds(2))
            got.get() > 0
        }
    }

    @Test
    fun `drain-delivers-inflight`() {
        val d = work()
        val file = tokenFile(d, "token-shop-api")
        val sub = connect(tokenConfig("drain-sub", file))
        val pub = connect(tokenConfig("drain-pub", file))
        val n = 200
        val got = AtomicInteger()
        val subject = subj("drain")
        sub.connection.createDispatcher { Thread.sleep(1); got.incrementAndGet() }.subscribe(subject)
        sub.connection.flush(Duration.ofSeconds(5))
        repeat(n) { pub.connection.publish(subject, "x".toByteArray()) }
        pub.connection.flush(Duration.ofSeconds(5))
        sub.close()
        assertEquals(n, got.get(), "every message delivered to the subscriber before the drain started is handled")
        assertEquals(Connection.Status.CLOSED, sub.connection.status)
    }

    @Test
    fun `jetstream-defaults`() {
        val d = work()
        val c = connect(tokenConfig("jetstream-defaults", tokenFile(d, "token-shop-api")))
        val name = "DEFAULTS${System.nanoTime()}"
        val spec = StreamSpec(name, listOf(subj("defaults.>")))
        val cfg = c.ensureStream(spec).configuration
        assertEquals(StorageType.File, cfg.storageType)
        assertEquals(RetentionPolicy.Limits, cfg.retentionPolicy)
        assertEquals(DiscardPolicy.Old, cfg.discardPolicy)
        assertEquals(1, cfg.replicas)
        assertEquals(Duration.ofDays(7), cfg.maxAge)
        assertEquals(Duration.ofMinutes(2), cfg.duplicateWindow)
        // Idempotent, and a changed spec updates the stream.
        c.ensureStream(spec)
        assertEquals(Duration.ofHours(1), c.ensureStream(spec.copy(maxAge = Duration.ofHours(1))).configuration.maxAge)

        val cc = c.ensureConsumer(ConsumerSpec(name, "worker", filterSubject = subj("defaults.a"))).consumerConfiguration
        assertEquals("worker", cc.durable)
        assertEquals(AckPolicy.Explicit, cc.ackPolicy)
        assertEquals(Duration.ofSeconds(30), cc.ackWait)
        assertEquals(5L, cc.maxDeliver)
        assertEquals(1000L, cc.maxAckPending)
        assertEquals(DeliverPolicy.All, cc.deliverPolicy)
        assertEquals(subj("defaults.a"), cc.filterSubject)
        c.ensureConsumer(ConsumerSpec(name, "worker", filterSubject = subj("defaults.a")))
    }

    @Test
    fun `jetstream-publish-ack-dedup`() {
        val d = work()
        val c = connect(tokenConfig("jetstream-publish-ack-dedup", tokenFile(d, "token-shop-api")))
        val name = "DEDUP${System.nanoTime()}"
        c.ensureStream(StreamSpec(name, listOf(subj("dedup.>"))))
        val first = c.publish(subj("dedup.a"), "1".toByteArray(), PublishOptions(msgId = "m-1"))
        assertFalse(first.isDuplicate)
        val second = c.publish(subj("dedup.a"), "1".toByteArray(), PublishOptions(msgId = "m-1"))
        assertTrue(second.isDuplicate)
        assertEquals(first.seqno, second.seqno)
        assertThrows(IllegalArgumentException::class.java, { c.publish(subj("dedup.*"), "1".toByteArray()) }, "a wildcard subject is refused before it is sent")
    }

    @Test
    fun `cert-identity-publish-limited`() {
        // The stream is made by a token client; the certificate identity may only publish.
        val d = work(certs)
        val admin = connect(tokenConfig("cert-limited-admin", tokenFile(d, "token-shop-api")))
        val name = "LIMITED${System.nanoTime()}"
        admin.ensureStream(StreamSpec(name, listOf(subj("limited.>"), "forbidden.$run.>")))

        val c = connect(certConfig("cert-identity-publish-limited", d, "api"))
        val ack = c.publish(subj("limited.ok"), "x".toByteArray())
        assertEquals(name, ack.stream)
        assertThrows(NatsClientException::class.java, {
            c.publish("forbidden.$run.no", "x".toByteArray(), PublishOptions(timeout = Duration.ofSeconds(2)))
        }, "the identity may not publish outside its subjects")
    }

    @Test
    fun `propagates-trace-headers`() {
        val d = work()
        val c = connect(tokenConfig("propagates-trace-headers", tokenFile(d, "token-shop-api")))
        val name = "TRACE${System.nanoTime()}"
        c.ensureStream(StreamSpec(name, listOf(subj("trace.>"))))

        val tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        val ack = c.publish(subj("trace.a"), "x".toByteArray(), PublishOptions(traceparent = tp, tracestate = "k=v"))

        // What is on the wire: the lower-case keys, exactly, read by the driver without the adapter.
        val wire = c.jetStreamManagement().getMessage(name, ack.seqno).headers!!
        assertNotNull(wire)
        assertTrue(wire.containsKey("traceparent"), "traceparent is written lower-case")
        assertFalse(wire.containsKey("Traceparent"), "and not in the HTTP-canonical spelling")
        assertEquals(tp to "k=v", extractTrace(wire))

        // A message another language wrote with the canonical spelling is still read.
        val other = Headers().add("Traceparent", tp).add("Tracestate", "a=b")
        assertEquals(tp to "a=b", extractTrace(other))
        // Setting replaces every spelling.
        injectTrace(other, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00", "")
        assertEquals(listOf("traceparent"), HeaderCarrier(other).keys())
    }

    @Test
    fun `health-check`() {
        val d = work()
        val c = connect(tokenConfig("health-check", tokenFile(d, "token-shop-api")))
        val start = System.nanoTime()
        c.health()
        assertTrue(Duration.ofNanos(System.nanoTime() - start) < Duration.ofSeconds(2))
        c.close()
        assertThrows(NatsClientException::class.java, { c.health() }, "a closed connection is not healthy")
    }

    @Test
    fun `account-for-namespace-vectors`() = checkAccountVectors()

    @Test
    fun `subject-vectors`() = checkSubjectVectors()
}
