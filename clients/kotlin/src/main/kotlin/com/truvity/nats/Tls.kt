package com.truvity.nats

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyFactory
import java.security.KeyStore
import java.security.Principal
import java.security.PrivateKey
import java.security.Provider
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.KeyManager
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLContextSpi
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLParameters
import javax.net.ssl.SSLServerSocketFactory
import javax.net.ssl.SSLSessionContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509ExtendedKeyManager
import javax.net.ssl.X509ExtendedTrustManager

/**
 * A TLS connection failed verification or the handshake (an unknown CA, a name the certificate
 * does not carry, a refused client certificate). Never retried.
 */
class NatsTlsException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * An [SSLContext] that is verify-full against the CA file and re-reads the CA, the client
 * certificate and the key for EVERY new connection.
 *
 * jnats builds its SSLContext once, in `Options.Builder.build()`, and takes the socket factory from
 * it for each (re)connect (`SocketDataPort.upgradeToSecure`: `context.getSocketFactory()` then
 * `createSocket(socket, host, port, true)`). So the context handed to jnats is this thin shell
 * whose socket factory builds a fresh, real context from the files at each `createSocket`; the
 * rotated files are therefore used by the next reconnect and nothing is cached across them.
 *
 * Verification is never skipped: trust is ONLY the CA file (the JVM's own store is not consulted),
 * the chain is checked by the JDK's PKIX trust manager, and the name is checked against [host]
 * (endpoint identification "HTTPS", which also sets the SNI name). TLS 1.2 or later.
 */
internal class ReloadingSslContext private constructor(private val reloading: ReloadingSpi) :
    SSLContext(reloading, SSLContext.getDefault().provider, "TLS") {

    /** The last verification failure of the most recent handshake, for the error the caller sees. */
    val lastVerifyFailure: AtomicReference<Throwable?> get() = reloading.lastVerifyFailure

    /** Whether the most recent handshake got as far as sending our certificate (so the server's was verified). */
    val certificateSent: Boolean get() = reloading.certificateSent.get()

    /** Whether this context presents a client certificate. */
    val presentsClientCertificate: Boolean get() = reloading.certFile != null

    companion object {
        fun create(caFile: String, certFile: String?, keyFile: String?, host: String): ReloadingSslContext {
            val spi = ReloadingSpi(caFile, certFile, keyFile, host)
            // Fail now, with the file's name, if the files are unusable at start.
            spi.fresh()
            return ReloadingSslContext(spi)
        }
    }
}

private class ReloadingSpi(
    private val caFile: String,
    val certFile: String?,
    private val keyFile: String?,
    private val host: String,
) : SSLContextSpi() {
    val lastVerifyFailure = AtomicReference<Throwable?>()
    val certificateSent = java.util.concurrent.atomic.AtomicBoolean(false)

    /** A real context built from the files as they are right now. */
    fun fresh(): SSLContext {
        val roots = readCertificates(caFile, "the CA file")
        check(roots.isNotEmpty()) { "nats-client: the CA file $caFile holds no certificate" }
        val ks = KeyStore.getInstance(KeyStore.getDefaultType())
        ks.load(null, null)
        roots.forEachIndexed { i, c -> ks.setCertificateEntry("ca-$i", c) }
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        tmf.init(ks)
        val tm = tmf.trustManagers.filterIsInstance<X509ExtendedTrustManager>().first()
        val km: Array<KeyManager>? =
            if (certFile != null && keyFile != null) arrayOf(PemKeyManager.load(certFile, keyFile) { certificateSent.set(true) }) else null
        val ctx = SSLContext.getInstance("TLS")
        ctx.init(km, arrayOf<TrustManager>(RecordingTrustManager(tm, lastVerifyFailure)), SecureRandom())
        return ctx
    }

    private fun params(ctx: SSLContext): SSLParameters {
        val p = ctx.defaultSSLParameters
        p.protocols = arrayOf("TLSv1.3", "TLSv1.2")
        // Name verification against the URL's host, done by the trust manager.
        p.endpointIdentificationAlgorithm = "HTTPS"
        if (!isIpLiteral(host)) p.serverNames = listOf(SNIHostName(host))
        return p
    }

    fun factory(): SSLSocketFactory = object : SSLSocketFactory() {
        private fun <T : Socket> T.configured(ctx: SSLContext): T {
            (this as SSLSocket).sslParameters = params(ctx)
            return this
        }

        override fun createSocket(s: Socket, h: String?, port: Int, autoClose: Boolean): Socket {
            val ctx = fresh()
            lastVerifyFailure.set(null)
            return ctx.socketFactory.createSocket(s, host, port, autoClose).configured(ctx)
        }

        override fun createSocket(h: String?, port: Int): Socket {
            val ctx = fresh()
            return ctx.socketFactory.createSocket(host, port).configured(ctx)
        }

        override fun createSocket(h: String?, port: Int, la: InetAddress?, lp: Int): Socket {
            val ctx = fresh()
            return ctx.socketFactory.createSocket(host, port, la, lp).configured(ctx)
        }

        override fun createSocket(a: InetAddress?, port: Int): Socket {
            val ctx = fresh()
            return ctx.socketFactory.createSocket(a, port).configured(ctx)
        }

        override fun createSocket(a: InetAddress?, port: Int, la: InetAddress?, lp: Int): Socket {
            val ctx = fresh()
            return ctx.socketFactory.createSocket(a, port, la, lp).configured(ctx)
        }

        override fun getDefaultCipherSuites(): Array<String> = fresh().socketFactory.defaultCipherSuites
        override fun getSupportedCipherSuites(): Array<String> = fresh().socketFactory.supportedCipherSuites
    }

    override fun engineInit(km: Array<out KeyManager>?, tm: Array<out TrustManager>?, sr: SecureRandom?) = Unit
    override fun engineGetSocketFactory(): SSLSocketFactory = factory()
    override fun engineGetServerSocketFactory(): SSLServerSocketFactory =
        throw UnsupportedOperationException("client only")

    override fun engineCreateSSLEngine(): SSLEngine = engineCreateSSLEngine(host, -1)
    override fun engineCreateSSLEngine(h: String?, port: Int): SSLEngine {
        val ctx = fresh()
        val e = ctx.createSSLEngine(host, port)
        e.sslParameters = params(ctx)
        e.useClientMode = true
        return e
    }

    override fun engineGetServerSessionContext(): SSLSessionContext = fresh().serverSessionContext
    override fun engineGetClientSessionContext(): SSLSessionContext = fresh().clientSessionContext
    override fun engineGetDefaultSSLParameters(): SSLParameters = params(fresh())
    override fun engineGetSupportedSSLParameters(): SSLParameters = fresh().supportedSSLParameters
}

private fun isIpLiteral(h: String): Boolean =
    h.contains(':') || h.matches(Regex("[0-9]{1,3}(\\.[0-9]{1,3}){3}"))

/** Delegates verification to the JDK's trust manager and remembers why it refused. */
private class RecordingTrustManager(
    private val d: X509ExtendedTrustManager,
    private val last: AtomicReference<Throwable?>,
) : X509ExtendedTrustManager() {
    private inline fun <T> record(f: () -> T): T =
        try {
            f()
        } catch (e: CertificateException) {
            last.set(e)
            throw e
        }

    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String, socket: Socket?) =
        record { d.checkServerTrusted(chain, authType, socket) }

    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String, engine: SSLEngine?) =
        record { d.checkServerTrusted(chain, authType, engine) }

    override fun checkServerTrusted(chain: Array<out X509Certificate>, authType: String) =
        record { d.checkServerTrusted(chain, authType) }

    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String, socket: Socket?) =
        d.checkClientTrusted(chain, authType, socket)

    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String, engine: SSLEngine?) =
        d.checkClientTrusted(chain, authType, engine)

    override fun checkClientTrusted(chain: Array<out X509Certificate>, authType: String) =
        d.checkClientTrusted(chain, authType)

    override fun getAcceptedIssuers(): Array<X509Certificate> = d.acceptedIssuers
}

/**
 * Always offers the one client certificate it was loaded with, whatever issuers the server lists
 * (the JDK's own key manager withholds a certificate whose issuer the server did not name, which
 * would turn a wrong-authority certificate into "no certificate" and hide the real cause).
 */
private class PemKeyManager(
    private val chain: Array<X509Certificate>,
    private val key: PrivateKey,
    private val onOffered: () -> Unit,
) : X509ExtendedKeyManager() {
    override fun chooseClientAlias(keyType: Array<out String>?, issuers: Array<out Principal>?, socket: Socket?) = ALIAS.also { onOffered() }
    override fun chooseEngineClientAlias(keyType: Array<out String>?, issuers: Array<out Principal>?, engine: SSLEngine?) = ALIAS.also { onOffered() }
    override fun getClientAliases(keyType: String?, issuers: Array<out Principal>?) = arrayOf(ALIAS)
    override fun getCertificateChain(alias: String?) = chain.copyOf()
    override fun getPrivateKey(alias: String?) = key
    override fun getServerAliases(keyType: String?, issuers: Array<out Principal>?): Array<String>? = null
    override fun chooseServerAlias(keyType: String?, issuers: Array<out Principal>?, socket: Socket?): String? = null

    companion object {
        private const val ALIAS = "client"

        fun load(certFile: String, keyFile: String, onOffered: () -> Unit): PemKeyManager {
            val chain = readCertificates(certFile, "the client certificate file")
            check(chain.isNotEmpty()) { "nats-client: the client certificate file $certFile holds no certificate" }
            val key = readPrivateKey(keyFile)
            return PemKeyManager(chain.toTypedArray(), key, onOffered)
        }
    }
}

private fun readCertificates(file: String, what: String): List<X509Certificate> {
    val bytes = try {
        Files.readAllBytes(Path.of(file))
    } catch (e: IOException) {
        throw IOException("nats-client: read $what $file: ${e.message}", e)
    }
    val cf = CertificateFactory.getInstance("X.509")
    return try {
        cf.generateCertificates(ByteArrayInputStream(bytes)).filterIsInstance<X509Certificate>()
    } catch (e: CertificateException) {
        throw CertificateException("nats-client: $what $file is not PEM certificates: ${e.message}", e)
    }
}

/**
 * Reads a PEM private key: PKCS#8 (`PRIVATE KEY`), PKCS#1 RSA (`RSA PRIVATE KEY`) or SEC1 EC
 * (`EC PRIVATE KEY`, what cert-manager writes by default for an ECDSA key). The JDK only parses
 * PKCS#8, so the other two are wrapped. An encrypted key is refused. The key never appears in a
 * string that outlives this function or in an error.
 */
internal fun readPrivateKey(file: String): PrivateKey {
    val pem = try {
        String(Files.readAllBytes(Path.of(file)), Charsets.US_ASCII)
    } catch (e: IOException) {
        throw IOException("nats-client: read the key file $file: ${e.message}", e)
    }
    val m = Regex("-----BEGIN ([A-Z0-9 ]+)-----([A-Za-z0-9+/=\\s]+)-----END \\1-----").find(pem)
        ?: throw IllegalStateException("nats-client: the key file $file holds no PEM private key")
    val der = Base64.getMimeDecoder().decode(m.groupValues[2])
    val pkcs8 = when (m.groupValues[1]) {
        "PRIVATE KEY" -> der
        "RSA PRIVATE KEY" -> Der.pkcs8(Der.RSA_ALGORITHM, der)
        "EC PRIVATE KEY" -> Der.pkcs8(Der.ecAlgorithm(der), der)
        "ENCRYPTED PRIVATE KEY" ->
            throw IllegalStateException("nats-client: the key file $file is encrypted; give an unencrypted key")
        else -> throw IllegalStateException("nats-client: the key file $file holds a ${m.groupValues[1]}, not a private key")
    }
    val spec = PKCS8EncodedKeySpec(pkcs8)
    for (alg in listOf("EC", "RSA", "Ed25519")) {
        try {
            return KeyFactory.getInstance(alg).generatePrivate(spec)
        } catch (_: java.security.spec.InvalidKeySpecException) {
            // try the next algorithm
        }
    }
    throw IllegalStateException("nats-client: the key file $file is not a usable RSA, EC or Ed25519 private key")
}

/** Just enough DER to wrap a PKCS#1 or SEC1 key as PKCS#8. */
private object Der {
    val RSA_ALGORITHM: ByteArray = seq(oid(byteArrayOf(0x2A, 0x86.toByte(), 0x48, 0x86.toByte(), 0xF7.toByte(), 0x0D, 0x01, 0x01, 0x01)) + byteArrayOf(0x05, 0x00))
    private val ID_EC_PUBLIC_KEY = oid(byteArrayOf(0x2A, 0x86.toByte(), 0x48, 0xCE.toByte(), 0x3D, 0x02, 0x01))

    private fun len(n: Int): ByteArray = when {
        n < 0x80 -> byteArrayOf(n.toByte())
        n < 0x100 -> byteArrayOf(0x81.toByte(), n.toByte())
        else -> byteArrayOf(0x82.toByte(), (n shr 8).toByte(), n.toByte())
    }

    private fun tlv(tag: Int, body: ByteArray): ByteArray =
        ByteArrayOutputStream().apply { write(tag); write(len(body.size)); write(body) }.toByteArray()

    fun seq(body: ByteArray) = tlv(0x30, body)
    fun oid(body: ByteArray) = tlv(0x06, body)

    fun pkcs8(algorithm: ByteArray, inner: ByteArray): ByteArray =
        seq(tlv(0x02, byteArrayOf(0)) + algorithm + tlv(0x04, inner))

    /** The AlgorithmIdentifier for an SEC1 ECPrivateKey: id-ecPublicKey plus the curve it names. */
    fun ecAlgorithm(sec1: ByteArray): ByteArray {
        // ECPrivateKey ::= SEQUENCE { version, privateKey OCTET STRING, [0] parameters, [1] publicKey }
        var p = 0
        fun readLen(): Int {
            val b = sec1[p++].toInt() and 0xFF
            if (b < 0x80) return b
            var n = 0
            repeat(b and 0x7F) { n = (n shl 8) or (sec1[p++].toInt() and 0xFF) }
            return n
        }
        require(sec1[p++].toInt() == 0x30) { "nats-client: malformed EC private key" }
        readLen()
        while (p < sec1.size) {
            val tag = sec1[p++].toInt() and 0xFF
            val l = readLen()
            if (tag == 0xA0) {
                // [0] holds the curve OID (a named curve).
                val curve = sec1.copyOfRange(p, p + l)
                require(curve.isNotEmpty() && curve[0].toInt() == 0x06) { "nats-client: the EC key names no curve" }
                return seq(ID_EC_PUBLIC_KEY + curve)
            }
            p += l
        }
        throw IllegalArgumentException("nats-client: the EC key names no curve")
    }
}
