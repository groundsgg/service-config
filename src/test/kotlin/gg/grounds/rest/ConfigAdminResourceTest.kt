package gg.grounds.rest

import gg.grounds.api.ConfigAdminDocumentService
import gg.grounds.auth.ConfigWritePolicy
import gg.grounds.grpc.config.ConfigDocument
import gg.grounds.grpc.config.DeleteDocumentResponse
import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.grpc.config.PutDocumentResponse
import jakarta.ws.rs.core.SecurityContext
import java.security.Principal
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever

class ConfigAdminResourceTest {
    private val service: ConfigAdminDocumentService = mock()
    private val resource =
        ConfigAdminResource(service, ConfigWritePolicy(":forge=network/stage/resourcepacks/global"))
    private val forge = security("system:serviceaccount:games:forge")

    @Test
    fun `exact writer puts normalized configured document`() {
        whenever(service.putDocument(any()))
            .thenReturn(PutDocumentResponse.newBuilder().setVersion(7).build())

        val result =
            resource.put(
                " network ",
                " stage ",
                " resourcepacks ",
                " global ",
                PutDocumentBody("{}"),
                forge,
            )

        assertEquals(7, (result.entity as WriteResultResponse).version)
        assertEquals("\"7\"", result.getHeaderString("ETag"))
        verify(service)
            .putDocument(
                org.mockito.kotlin.check {
                    assertEquals("network", it.app)
                    assertEquals("stage", it.env)
                    assertEquals("resourcepacks", it.namespace)
                    assertEquals("global", it.configKey)
                    assertEquals(false, it.hasExpectedVersion())
                }
            )
    }

    @Test
    fun `exact writer deletes its configured document`() {
        whenever(service.deleteDocument(any()))
            .thenReturn(DeleteDocumentResponse.newBuilder().setDeleted(true).setVersion(8).build())

        val result = resource.delete(" network ", " stage ", " resourcepacks ", " global ", forge)

        assertEquals(true, result.deleted)
        verify(service)
            .deleteDocument(
                org.mockito.kotlin.check {
                    assertEquals("network", it.app)
                    assertEquals("stage", it.env)
                    assertEquals("resourcepacks", it.namespace)
                    assertEquals("global", it.configKey)
                }
            )
    }

    @Test
    fun `exact writer cannot put a neighboring document`() {
        assertThrows(ForbiddenException::class.java) {
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "neighbor",
                PutDocumentBody("{}"),
                forge,
            )
        }

        verifyNoInteractions(service)
    }

    @Test
    fun `exact writer cannot delete in a neighboring environment`() {
        assertThrows(ForbiddenException::class.java) {
            resource.delete("network", "prod", "resourcepacks", "global", forge)
        }

        verifyNoInteractions(service)
    }

    @Test
    fun `admin document read carries the document version as a strong etag`() {
        whenever(service.getDocument(any()))
            .thenReturn(
                GetDocumentResponse.newBuilder()
                    .setDocument(
                        ConfigDocument.newBuilder()
                            .setNamespace("resourcepacks")
                            .setConfigKey("global")
                            .setContentJson("{}")
                            .setVersion(Long.MAX_VALUE)
                    )
                    .build()
            )

        val response =
            resource.get(
                "network",
                "stage",
                "resourcepacks",
                "global",
                security("system:serviceaccount:platform-admin:config-admin"),
            )

        assertEquals(200, response.status)
        assertEquals("\"${Long.MAX_VALUE}\"", response.getHeaderString("ETag"))
        assertEquals(Long.MAX_VALUE, (response.entity as ConfigDocumentResponse).version)
    }

    @Test
    fun `if match supplies a lossless expected version and the successful write etag`() {
        whenever(service.putDocument(any()))
            .thenReturn(PutDocumentResponse.newBuilder().setVersion(Long.MAX_VALUE).build())

        val response =
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}"),
                forge,
                " \"9007199254740993\" ",
            )

        assertEquals(Long.MAX_VALUE, (response.entity as WriteResultResponse).version)
        assertEquals("\"${Long.MAX_VALUE}\"", response.getHeaderString("ETag"))
        verify(service)
            .putDocument(
                org.mockito.kotlin.check {
                    assertEquals(true, it.hasExpectedVersion())
                    assertEquals(9007199254740993L, it.expectedVersion)
                }
            )
    }

    @Test
    fun `body expected version remains a compatible conditional write path`() {
        whenever(service.putDocument(any()))
            .thenReturn(PutDocumentResponse.newBuilder().setVersion(9).build())

        val response =
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}", expectedVersion = 8),
                forge,
            )

        assertEquals(9, (response.entity as WriteResultResponse).version)
        verify(service)
            .putDocument(
                org.mockito.kotlin.check {
                    assertEquals(true, it.hasExpectedVersion())
                    assertEquals(8, it.expectedVersion)
                }
            )
    }

    @Test
    fun `body and if match versions are mutually exclusive`() {
        assertThrows(InvalidRequestException::class.java) {
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}", expectedVersion = 7),
                forge,
                "\"7\"",
            )
        }

        verifyNoInteractions(service)
    }

    private fun security(subject: String): SecurityContext =
        mock<SecurityContext>().also { security ->
            whenever(security.userPrincipal).thenReturn(Principal { subject })
        }
}
