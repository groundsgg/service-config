package gg.grounds.rest

import gg.grounds.api.ConfigDocumentApiService
import gg.grounds.domain.ConfigDocument
import java.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class DocumentEtagTest {
    private val service: ConfigDocumentApiService = mock()
    private val resource = ConfigResource(service)

    private fun document(updatedBy: String?): ConfigDocument =
        ConfigDocument(
            app = "network",
            env = "stage",
            namespace = "resourcepacks",
            configKey = "global",
            contentJson = "{}",
            version = 9007199254740993L,
            updatedAt = Instant.parse("2026-10-16T15:46:24Z"),
            updatedBy = updatedBy,
        )

    @Test
    fun `public document read carries a lossless strong etag and its last write`() {
        whenever(service.getDocument(any())).thenReturn(document("keycloak-sub"))

        val response = resource.document("network", "stage", "resourcepacks", "global")

        assertEquals(200, response.status)
        assertEquals("\"9007199254740993\"", response.getHeaderString("ETag"))
        val actualBody = response.entity as ConfigDocumentDetailResponse
        assertEquals(9007199254740993L, actualBody.version)
        assertEquals("resourcepacks", actualBody.namespace)
        assertEquals("global", actualBody.configKey)
        assertEquals("{}", actualBody.contentJson)
        assertEquals("2026-10-16T15:46:24Z", actualBody.updatedAt)
        assertEquals("keycloak-sub", actualBody.updatedBy)
    }

    @Test
    fun `public document read reports an unrecorded author as null`() {
        whenever(service.getDocument(any())).thenReturn(document(null))

        val response = resource.document("network", "stage", "resourcepacks", "global")

        assertNull((response.entity as ConfigDocumentDetailResponse).updatedBy)
    }

    @Test
    fun `if match accepts exactly one strong canonical positive signed long`() {
        assertEquals(9007199254740993L, parseIfMatch(" \"9007199254740993\" "))
        assertEquals(42L, parseIfMatch("\t\"42\"\t"))
        assertEquals(Long.MAX_VALUE, parseIfMatch("\"${Long.MAX_VALUE}\""))
    }

    @Test
    fun `if match rejects weak bare wildcard list overflow noncanonical and nonpositive values`() {
        listOf(
                "W/\"7\"",
                "7",
                "*",
                "\"7\", \"8\"",
                "\"9223372036854775808\"",
                "\"07\"",
                "\"+7\"",
                "\"0\"",
                "\"-1\"",
                "\r\"7\"",
                "\"7\"\n",
                "\u00a0\"7\"",
            )
            .forEach { header ->
                assertThrows(InvalidRequestException::class.java) { parseIfMatch(header) }
            }
    }
}
