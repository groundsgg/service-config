package gg.grounds.events

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.nio.file.Path
import java.util.logging.Handler
import java.util.logging.LogRecord
import java.util.logging.Logger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class ConfigChangePublisherTest {
    private val objectMapper = ObjectMapper()
    @TempDir lateinit var tempDir: Path

    @Test
    fun `publishChange returns skipped when NATS is not connected`() {
        val publisher = ConfigChangePublisher("nats://localhost:4222", objectMapper)

        val result =
            publisher.publishChange(
                app = "lobby",
                env = "prod",
                version = 42,
                namespace = "lobby",
                configKey = "settings",
            )

        assertEquals(ConfigChangePublisher.PublishChangeResult.SKIPPED_NOT_CONNECTED, result)
    }

    @Test
    fun `buildPayload uses configKey field name`() {
        val publisher = ConfigChangePublisher("nats://localhost:4222", objectMapper)

        val payload =
            publisher.buildPayload(
                app = "lobby",
                env = "prod",
                version = 42,
                namespace = "lobby",
                configKey = "settings",
            )
        val json = objectMapper.readTree(payload)

        assertEquals("settings", json["configKey"].asText())
        assertFalse(json.has("config_key"))
    }

    @Test
    fun `projected token mode rotates the NATS bearer and keeps connection settings`() {
        val tokenFile = tempDir.resolve("projected-token")
        Files.writeString(tokenFile, "first-synthetic-token\n")
        val publisher =
            publisher(
                natsUrl = "nats://options-test.invalid:4222",
                groundsTokenFile = tokenFile.toString(),
            )

        val options = publisher.buildConnectionOptions()

        assertEquals("first-synthetic-token", options.tokenChars.concatToString())
        Files.writeString(tokenFile, "second-synthetic-token\n")
        assertEquals("second-synthetic-token", options.tokenChars.concatToString())
        assertEquals("nats://options-test.invalid:4222", options.servers.single().toString())
        assertEquals(7, options.maxReconnect)
        assertEquals(3_000, options.reconnectWait.toMillis())
        assertFalse(options.connectionListener == null)
    }

    @Test
    fun `static mode leaves NATS bearer unset and keeps authenticated server settings`() {
        val tokenFile = tempDir.resolve("projected-token")
        Files.writeString(tokenFile, "must-not-be-read")
        val publisher =
            publisher(
                natsUrl = "nats://synthetic-user:synthetic-password@options-test.invalid:4222",
                groundsTokenFile = tokenFile.toString(),
                natsAuthMode = "static",
            )

        val options = publisher.buildConnectionOptions()

        assertNull(options.tokenChars)
        assertEquals(
            "nats://synthetic-user:synthetic-password@options-test.invalid:4222",
            options.servers.single().toString(),
        )
        assertEquals(7, options.maxReconnect)
        assertEquals(3_000, options.reconnectWait.toMillis())
        assertFalse(options.connectionListener == null)
    }

    @Test
    fun `unsupported NATS authentication mode is rejected before connecting`() {
        val publisher = publisher(natsAuthMode = "automatic")

        val error =
            assertThrows(IllegalArgumentException::class.java) {
                publisher.buildConnectionOptions()
            }

        assertEquals("Unsupported NATS authentication mode: automatic", error.message)
    }

    @Test
    fun `malformed authenticated URL logs no credentials or throwable before any network connection`() {
        val credentialSentinel = "publisher-log-credential-sentinel"
        val malformedUrl = "nats://synthetic-user:$credentialSentinel@["
        val publisher = publisher(natsUrl = malformedUrl, natsAuthMode = "static")
        val logger = Logger.getLogger(ConfigChangePublisher::class.java.name)
        val capture = CapturingHandler()
        logger.addHandler(capture)
        try {
            publisher.connect()
        } finally {
            logger.removeHandler(capture)
        }

        assertFalse(publisher.isConnected())
        assertFalse(capture.records.isEmpty())
        assertFalse(capture.records.any { it.message.contains(credentialSentinel) })
        assertFalse(capture.records.any { it.message.contains(malformedUrl) })
        assertFalse(capture.records.any { it.thrown != null })
    }

    private fun publisher(
        natsUrl: String = "nats://options-test.invalid:4222",
        groundsTokenFile: String = tempDir.resolve("missing-token").toString(),
        natsAuthMode: String = "projected-token",
    ) =
        ConfigChangePublisher(
            natsUrl = natsUrl,
            maxReconnects = 7,
            reconnectWaitSeconds = 3,
            groundsTokenFile = groundsTokenFile,
            objectMapper = objectMapper,
            natsAuthMode = natsAuthMode,
        )

    private class CapturingHandler : Handler() {
        val records = mutableListOf<LogRecord>()

        override fun publish(record: LogRecord) {
            records += record
        }

        override fun flush() = Unit

        override fun close() = Unit
    }
}
