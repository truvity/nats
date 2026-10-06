package com.truvity.nats

import io.nats.client.impl.Headers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.ConnectException
import java.nio.file.Files
import java.security.KeyPairGenerator
import java.security.interfaces.ECPrivateKey
import java.time.Duration
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

class ConfigTest {
    private fun cfg(vararg kv: Pair<String, String>): NatsConfig = NatsConfig.fromEnv(mapOf(*kv)::get)

    @Test
    fun `defaults and env`() {
        val c = cfg(
            "NATS_URL" to "tls://nats.example:4222", "NATS_CA_FILE" to "/ca.crt", "NATS_TOKEN_FILE" to "/token",
            "NATS_CLIENT_NAME" to "svc", "NATS_CLIENT_CONNECT_TIMEOUT" to "500ms", "NATS_CLIENT_RECONNECT_WAIT" to "1m30s",
            "NATS_CLIENT_RETRY_ATTEMPTS" to "3", "NATS_CLIENT_RETRY_MAX_DELAY" to "2s", "NATS_CLIENT_RETRY_BUDGET" to "10s",
        )
        assertEquals("svc", c.name)
        assertEquals(Duration.ofMillis(500), c.connectTimeout)
        assertEquals(Duration.ofSeconds(90), c.reconnectWait)
        assertEquals(Duration.ofSeconds(30), c.pingInterval)
        assertEquals(Duration.ofSeconds(10), c.drainTimeout)
        assertEquals(RetryPolicy(3, Duration.ofMillis(200), Duration.ofSeconds(2), Duration.ofSeconds(10)), c.connect)
        assertTrue(c.tls)
    }

    @Test
    fun `lists every problem at once`() {
        val e = assertThrows(ConfigError::class.java) {
            NatsConfig(url = "tls://localhost:4222", certFile = "/x.crt", tokenFile = "/token").validate()
        }
        for (want in listOf("server CA", "go together", "two identities")) assertTrue(want in e.message!!, want)
        assertTrue(e.problems.size >= 3)
    }

    @Test
    fun `refuses a scheme, a second URL and credentials in the URL without echoing them`() {
        val scheme = assertThrows(ConfigError::class.java) { NatsConfig(url = "http://localhost:4222", tokenFile = "/t").validate() }
        assertTrue("scheme" in scheme.message!!)
        val two = assertThrows(ConfigError::class.java) { NatsConfig(url = "nats://a:4222,nats://b:4222", tokenFile = "/t").validate() }
        assertTrue("exactly one URL" in two.message!!)
        val creds = assertThrows(ConfigError::class.java) { NatsConfig(url = "nats://user:pw@localhost:4222", tokenFile = "/t").validate() }
        assertTrue("credentials in the URL" in creds.message!!)
        assertFalse("pw" in creds.message!!)
        assertEquals("nats://localhost:4222", redactUrl("nats://user:pw@localhost:4222"))
    }

    @Test
    fun `needs a credential and a CA with a certificate`() {
        assertTrue("a credential is required" in assertThrows(ConfigError::class.java) { NatsConfig(url = "nats://h:4222").validate() }.message!!)
        val e = assertThrows(ConfigError::class.java) { NatsConfig(url = "nats://h:4222", certFile = "/c", keyFile = "/k").validate() }
        assertTrue("needs the server CA" in e.message!!)
        NatsConfig(url = "nats://h:4222", tokenFile = "/t").validate()
        NatsConfig(url = "tls://h:4222", caFile = "/ca", certFile = "/c", keyFile = "/k").validate()
    }

    @Test
    fun `bad durations and non-positive values are errors`() {
        val e = assertThrows(ConfigError::class.java) {
            cfg("NATS_URL" to "nats://h:4222", "NATS_TOKEN_FILE" to "/t", "NATS_CLIENT_PING_INTERVAL" to "soon", "NATS_CLIENT_RETRY_ATTEMPTS" to "x")
        }
        assertTrue("NATS_CLIENT_PING_INTERVAL" in e.message!!)
        assertTrue("NATS_CLIENT_RETRY_ATTEMPTS" in e.message!!)
        assertThrows(ConfigError::class.java) { NatsConfig(url = "nats://h", tokenFile = "/t", drainTimeout = Duration.ZERO).validate() }
    }

    @Test
    fun `toString carries no credential`() {
        val s = NatsConfig(url = "nats://user:pw@localhost:4222", tokenFile = "/t").toString()
        assertFalse("pw" in s)
    }

    @Test
    fun `go style durations`() {
        assertEquals(Duration.ofMillis(1500), parseDuration("1.5s"))
        assertEquals(Duration.ofSeconds(3690), parseDuration("1h1m30s"))
        assertNull(parseDuration("5"))
        assertNull(parseDuration("5x"))
        assertNull(parseDuration(""))
    }
}

class RetryTest {
    private val fast = RetryPolicy(attempts = 4, initialDelay = Duration.ofMillis(1), maxDelay = Duration.ofMillis(4), budget = Duration.ofSeconds(5))

    @Test
    fun `retries a retryable error until it works`() {
        val n = AtomicInteger()
        val v = retry(fast) { if (n.incrementAndGet() < 3) throw ConnectException("refused") else "ok" }
        assertEquals("ok", v)
        assertEquals(3, n.get())
    }

    @Test
    fun `stops at the attempt count`() {
        val n = AtomicInteger()
        assertThrows(ConnectException::class.java) { retry(fast) { n.incrementAndGet(); throw ConnectException("refused") } }
        assertEquals(4, n.get())
    }

    @Test
    fun `stops at the budget`() {
        val n = AtomicInteger()
        val p = RetryPolicy(attempts = 100, initialDelay = Duration.ofMillis(50), maxDelay = Duration.ofMillis(50), budget = Duration.ofMillis(1))
        assertThrows(ConnectException::class.java) { retry(p) { n.incrementAndGet(); throw ConnectException("refused") } }
        assertTrue(n.get() < 100)
    }

    @Test
    fun `never retries authorization, certificates, permissions or interruption`() {
        val bad = listOf<Throwable>(
            io.nats.client.AuthenticationException("Authentication error connecting to NATS server: authorization violation"),
            NatsTlsException("certificate verification failed"),
            javax.net.ssl.SSLHandshakeException("PKIX path building failed"),
            IOException("Permissions Violation for Publish to x"),
            IOException("SSL connection wanted by client."),
            InterruptedException(),
            RuntimeException("wrapped", java.security.cert.CertificateException("x")),
        )
        for (t in bad) {
            val n = AtomicInteger()
            assertThrows(Throwable::class.java, { retry(fast) { n.incrementAndGet(); throw t } }, t.toString())
            assertEquals(1, n.get(), t.toString())
        }
    }

    @Test
    fun `classifies connection-class errors through the cause chain`() {
        assertTrue(isRetryable(ConnectException("refused")))
        assertTrue(isRetryable(IOException("Unable to connect to NATS servers: [nats://x]")))
        assertTrue(isRetryable(NatsConnectException("x", ConnectException(), true)))
        assertTrue(isRetryable(RuntimeException("wrapped", java.net.SocketTimeoutException())))
        assertFalse(isRetryable(IllegalStateException("plain")))
        assertFalse(isRetryable(null))
        assertFalse(isRetryable(NatsConnectException("x", null, false)))
    }

    @Test
    fun `an invalid policy is a ConfigError`() {
        assertThrows(ConfigError::class.java) { retry<Unit>(RetryPolicy(attempts = 0)) { } }
    }
}

class HeadersTest {
    @Test
    fun `writes lower case and replaces every spelling`() {
        val h = Headers().add("Traceparent", "old").add("TRACEPARENT", "older")
        injectTrace(h, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", "k=v")
        assertEquals(setOf("traceparent", "tracestate"), h.keySet())
        injectTrace(h, "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-00", "")
        assertEquals(listOf("traceparent"), HeaderCarrier(h).keys())
    }

    @Test
    fun `reads whatever case another language wrote`() {
        val h = Headers().add("Traceparent", "tp").add("Tracestate", "ts")
        assertEquals("tp" to "ts", extractTrace(h))
        assertEquals("" to "", extractTrace(Headers()))
        assertEquals("tp", HeaderCarrier(Headers().add("TRACEPARENT", "tp")).get("traceparent"))
    }
}

class VectorsTest {
    @Test
    fun `account for namespace vectors`() = checkAccountVectors()

    @Test
    fun `subject vectors`() = checkSubjectVectors()

    @Test
    fun `stream subjects may be wildcards but not reserved`() {
        validStreamSubject("orders.>")
        assertThrows(IllegalArgumentException::class.java) { validStreamSubject("\$JS.>") }
        assertThrows(IllegalArgumentException::class.java) { validStreamSubject("") }
    }
}

class PrivateKeyTest {
    private fun pem(label: String, der: ByteArray) =
        "-----BEGIN $label-----\n" + Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(der) + "\n-----END $label-----\n"

    private fun file(content: String) = Files.createTempFile("key", ".pem").also {
        Files.writeString(it, content)
        it.toFile().deleteOnExit()
    }

    private fun tlv(tag: Int, body: ByteArray): ByteArray {
        val len = if (body.size < 0x80) byteArrayOf(body.size.toByte()) else byteArrayOf(0x82.toByte(), (body.size shr 8).toByte(), body.size.toByte())
        return byteArrayOf(tag.toByte()) + len + body
    }

    @Test
    fun `reads PKCS8, PKCS1 RSA and SEC1 EC keys`() {
        val rsa = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair().private
        assertEquals("RSA", readPrivateKey(file(pem("PRIVATE KEY", rsa.encoded)).toString()).algorithm)
        // PKCS#1 is the OCTET STRING inside the PKCS#8 wrapper: after the 15-byte algorithm identifier.
        val p8 = rsa.encoded
        val algEnd = p8.indices.first { i -> i + 15 <= p8.size && p8[i] == 0x30.toByte() && p8[i + 1] == 0x0D.toByte() && p8[i + 2] == 0x06.toByte() } + 15
        assertEquals(0x04, p8[algEnd].toInt())
        val lenBytes = p8[algEnd + 1].toInt() and 0x7F
        val pkcs1 = p8.copyOfRange(algEnd + 2 + lenBytes, p8.size)
        assertEquals("RSA", readPrivateKey(file(pem("RSA PRIVATE KEY", pkcs1)).toString()).algorithm)

        val ec = KeyPairGenerator.getInstance("EC").apply { initialize(java.security.spec.ECGenParameterSpec("secp256r1")) }.generateKeyPair().private as ECPrivateKey
        val d = ec.s.toByteArray().let { if (it.size > 32) it.copyOfRange(it.size - 32, it.size) else ByteArray(32 - it.size) + it }
        val p256 = byteArrayOf(0x06, 0x08, 0x2A, 0x86.toByte(), 0x48, 0xCE.toByte(), 0x3D, 0x03, 0x01, 0x07)
        val sec1 = tlv(0x30, tlv(0x02, byteArrayOf(1)) + tlv(0x04, d) + tlv(0xA0, p256))
        val got = readPrivateKey(file(pem("EC PRIVATE KEY", sec1)).toString())
        assertEquals("EC", got.algorithm)
        assertEquals(ec.s, (got as ECPrivateKey).s)
    }

    @Test
    fun `refuses what is not a usable key without echoing it`() {
        assertThrows(IllegalStateException::class.java) { readPrivateKey(file("not a key").toString()) }
        val e = assertThrows(IllegalStateException::class.java) { readPrivateKey(file(pem("ENCRYPTED PRIVATE KEY", byteArrayOf(1, 2, 3))).toString()) }
        assertTrue("encrypted" in e.message!!)
        assertNotNull(e.message)
    }
}
