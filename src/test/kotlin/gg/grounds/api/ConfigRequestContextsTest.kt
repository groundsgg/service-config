package gg.grounds.api

import gg.grounds.domain.ConfigErrorCode
import gg.grounds.domain.ConfigException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ConfigRequestContextsTest {
    @Test
    fun `toAppEnvContext rejects blank app`() {
        val thrown =
            assertThrows(ConfigException::class.java) {
                ConfigRequestContexts.toAppEnvContext("   ", "prod")
            }

        assertEquals(ConfigErrorCode.INVALID_ARGUMENT, thrown.code)
        assertEquals("app must not be blank", thrown.message)
    }

    @Test
    fun `toAppEnvContext rejects invalid env segment`() {
        val thrown =
            assertThrows(ConfigException::class.java) {
                ConfigRequestContexts.toAppEnvContext("player", "prod.live")
            }

        assertEquals(ConfigErrorCode.INVALID_ARGUMENT, thrown.code)
        assertEquals("env must match [A-Za-z0-9_-]+", thrown.message)
    }

    @Test
    fun `toNamespaceContext allows empty namespace for wildcard lookups`() {
        val context =
            ConfigRequestContexts.toNamespaceContext(
                "player",
                "prod",
                "   ",
                allowEmptyNamespace = true,
            )

        assertEquals("player", context.app)
        assertEquals("prod", context.env)
        assertEquals("", context.namespace)
    }
}
