package com.truvity.nats

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.ServerSocket
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

/**
 * The reconnect that must not give up after repeated authorization errors (jnats would, after two
 * identical ones from the same server): a stand-in broker accepts the first connection, then drops
 * it and answers every later one with an authorization violation.
 */
class ReconnectTest {
    @Test
    fun `keeps reconnecting after repeated authorization errors`() {
        val accepted = AtomicInteger()
        ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress()).use { server ->
            val acceptor = thread(isDaemon = true) {
                while (!server.isClosed) {
                    val s = try { server.accept() } catch (_: Exception) { return@thread }
                    val n = accepted.incrementAndGet()
                    thread(isDaemon = true) {
                        s.use {
                            val out = it.getOutputStream()
                            val input = it.getInputStream().bufferedReader()
                            out.write(("INFO {\"server_id\":\"x\",\"server_name\":\"x\",\"version\":\"2.10.0\",\"proto\":1,\"go\":\"go1\"," +
                                "\"host\":\"127.0.0.1\",\"port\":${server.localPort},\"headers\":true,\"max_payload\":1048576," +
                                "\"auth_required\":true}\r\n").toByteArray())
                            out.flush()
                            while (true) {
                                val line = input.readLine() ?: break
                                if (line == "PING") {
                                    if (n == 1) {
                                        out.write("PONG\r\n".toByteArray()); out.flush()
                                        Thread.sleep(200)
                                    } else {
                                        out.write("-ERR 'Authorization Violation'\r\n".toByteArray()); out.flush()
                                    }
                                    break
                                }
                            }
                        }
                    }
                }
            }
            val tokenFile = Files.createTempFile("token", "")
            Files.writeString(tokenFile, "t")
            val c = NatsClient.connect(
                NatsConfig(
                    url = "nats://127.0.0.1:${server.localPort}", tokenFile = tokenFile.toString(), name = "reconnect-test",
                    reconnectWait = Duration.ofMillis(50), connectTimeout = Duration.ofMillis(500), drainTimeout = Duration.ofMillis(500),
                ),
            )
            try {
                val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
                while (accepted.get() < 6 && System.nanoTime() < deadline) Thread.sleep(50)
                assertTrue(accepted.get() >= 6, "only ${accepted.get()} connections: the driver gave up after authorization errors")
                assertTrue(c.connection.status != io.nats.client.Connection.Status.CLOSED, "the connection was closed")
            } finally {
                runCatching { c.close() }
                server.close()
                acceptor.join(1000)
            }
        }
    }
}
