package gg.grounds.rest

import gg.grounds.api.ConfigAdminDocumentService
import gg.grounds.auth.ConfigWritePolicy
import gg.grounds.grpc.config.DeleteDocumentResponse
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

        assertEquals(7, result.version)
        verify(service)
            .putDocument(
                org.mockito.kotlin.check {
                    assertEquals("network", it.app)
                    assertEquals("stage", it.env)
                    assertEquals("resourcepacks", it.namespace)
                    assertEquals("global", it.configKey)
                }
            )
    }

    @Test
    fun `exact writer deletes its configured document`() {
        whenever(service.deleteDocument(any()))
            .thenReturn(DeleteDocumentResponse.newBuilder().setDeleted(true).setVersion(8).build())

        val result =
            resource.delete(
                " network ",
                " stage ",
                " resourcepacks ",
                " global ",
                forge,
            )

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

    private fun security(subject: String): SecurityContext =
        mock<SecurityContext>().also { security ->
            whenever(security.userPrincipal).thenReturn(Principal { subject })
        }
}
