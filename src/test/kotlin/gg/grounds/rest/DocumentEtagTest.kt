package gg.grounds.rest

import gg.grounds.api.ConfigDocumentApiService
import gg.grounds.grpc.config.ConfigDocument
import gg.grounds.grpc.config.GetDocumentResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class DocumentEtagTest {
    private val service: ConfigDocumentApiService = mock()
    private val resource = ConfigResource(service)

    @Test
    fun `public document read carries a lossless strong etag`() {
        whenever(service.getDocument(any()))
            .thenReturn(
                GetDocumentResponse.newBuilder()
                    .setDocument(
                        ConfigDocument.newBuilder()
                            .setNamespace("resourcepacks")
                            .setConfigKey("global")
                            .setContentJson("{}")
                            .setVersion(9007199254740993L)
                    )
                    .build()
            )

        val response = resource.document("network", "stage", "resourcepacks", "global")

        assertEquals(200, response.status)
        assertEquals("\"9007199254740993\"", response.getHeaderString("ETag"))
        assertEquals(9007199254740993L, (response.entity as ConfigDocumentResponse).version)
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
        ).forEach { header ->
            assertThrows(InvalidRequestException::class.java) { parseIfMatch(header) }
        }
    }
}
