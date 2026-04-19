package gg.grounds.events

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test

class ConfigChangePublisherTest {
    private val objectMapper = ObjectMapper()

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
}
