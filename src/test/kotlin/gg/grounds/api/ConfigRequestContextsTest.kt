package gg.grounds.api

import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ConfigRequestContextsTest {
    @Test
    fun `toAppEnvContext rejects blank app`() {
        val thrown =
            assertThrows(StatusRuntimeException::class.java) {
                ConfigRequestContexts.toAppEnvContext("   ", "prod")
            }

        assertEquals(Status.Code.INVALID_ARGUMENT, thrown.status.code)
        assertEquals("app must not be blank", thrown.status.description)
    }

    @Test
    fun `toAppEnvContext rejects invalid env segment`() {
        val thrown =
            assertThrows(StatusRuntimeException::class.java) {
                ConfigRequestContexts.toAppEnvContext("player", "prod.live")
            }

        assertEquals(Status.Code.INVALID_ARGUMENT, thrown.status.code)
        assertEquals("env must match [A-Za-z0-9_-]+", thrown.status.description)
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
