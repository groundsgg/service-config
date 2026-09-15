package gg.grounds.rest

import com.fasterxml.jackson.databind.ObjectMapper
import gg.grounds.api.ConfigAdminDocumentService
import gg.grounds.auth.ConfigWritePolicy
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.ConfigDocument
import gg.grounds.grpc.config.DeleteDocumentResponse
import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.persistence.ConfigDocumentRepository
import jakarta.ws.rs.core.HttpHeaders
import jakarta.ws.rs.core.SecurityContext
import java.security.Principal
import java.util.AbstractMap.SimpleImmutableEntry
import org.jboss.resteasy.reactive.server.jaxrs.HttpHeadersImpl
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
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
        whenever(service.putDocumentWithVersion(any()))
            .thenReturn(ConfigAdminDocumentService.PutDocumentWithVersionResult(7, 4))

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
        assertEquals("\"4\"", result.getHeaderString("ETag"))
        verify(service)
            .putDocumentWithVersion(
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
        whenever(service.putDocumentWithVersion(any()))
            .thenReturn(
                ConfigAdminDocumentService.PutDocumentWithVersionResult(
                    Long.MAX_VALUE,
                    9007199254740993L,
                )
            )

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
        assertEquals("\"9007199254740993\"", response.getHeaderString("ETag"))
        verify(service)
            .putDocumentWithVersion(
                org.mockito.kotlin.check {
                    assertEquals(true, it.hasExpectedVersion())
                    assertEquals(9007199254740993L, it.expectedVersion)
                }
            )
    }

    @Test
    fun `body expected version remains a compatible conditional write path`() {
        whenever(service.putDocumentWithVersion(any()))
            .thenReturn(ConfigAdminDocumentService.PutDocumentWithVersionResult(11, 4))

        val response =
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}", expectedVersion = 8),
                forge,
            )

        assertEquals(11, (response.entity as WriteResultResponse).version)
        assertEquals("\"4\"", response.getHeaderString("ETag"))
        verify(service)
            .putDocumentWithVersion(
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

    @Test
    fun `raw empty if match with a body version is rejected without a write`() {
        assertThrows(InvalidRequestException::class.java) {
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}", expectedVersion = 3),
                forge,
                null,
                headers("If-Match" to ""),
            )
        }

        verifyNoInteractions(service)
    }

    @Test
    fun `raw repeated if match is rejected without a write`() {
        assertThrows(InvalidRequestException::class.java) {
            resource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}"),
                forge,
                "\"4\"",
                headers("if-match" to "\"4\"", "If-Match" to "\"5\""),
            )
        }

        verifyNoInteractions(service)
    }

    @Test
    fun `raw headers use the one case insensitive if match value`() {
        whenever(service.putDocumentWithVersion(any()))
            .thenReturn(ConfigAdminDocumentService.PutDocumentWithVersionResult(11, 4))

        resource.put(
            "network",
            "stage",
            "resourcepacks",
            "global",
            PutDocumentBody("{}"),
            forge,
            null,
            headers("if-match" to "\"4\""),
        )

        verify(service)
            .putDocumentWithVersion(
                org.mockito.kotlin.check { assertEquals(4, it.expectedVersion) }
            )
    }

    @Test
    fun `raw header context never falls back to the scalar if match value`() {
        whenever(service.putDocumentWithVersion(any()))
            .thenReturn(ConfigAdminDocumentService.PutDocumentWithVersionResult(11, 4))

        resource.put(
            "network",
            "stage",
            "resourcepacks",
            "global",
            PutDocumentBody("{}"),
            forge,
            "\"4\"",
            headers(),
        )

        verify(service)
            .putDocumentWithVersion(
                org.mockito.kotlin.check { assertEquals(false, it.hasExpectedVersion()) }
            )
    }

    @Test
    fun `put uses document etag while preserving the app version body and CAS sequence`() {
        val repository: ConfigDocumentRepository = mock()
        val publisher: ConfigChangePublisher = mock()
        val liveResource =
            ConfigAdminResource(
                ConfigAdminDocumentService(repository, publisher, ObjectMapper()),
                ConfigWritePolicy(":forge=network/stage/resourcepacks/global"),
            )
        whenever(repository.upsertAndIncrementVersion(any(), eq(3L)))
            .thenReturn(ConfigDocumentRepository.UpsertAndIncrementVersionResult.Updated(11, 4))
        whenever(repository.upsertAndIncrementVersion(any(), eq(4L)))
            .thenReturn(ConfigDocumentRepository.UpsertAndIncrementVersionResult.Updated(12, 5))
        whenever(publisher.publishChange(any(), any(), any(), any(), any()))
            .thenReturn(ConfigChangePublisher.PublishChangeResult.SKIPPED_NOT_CONNECTED)

        val first =
            liveResource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}", expectedVersion = 3),
                forge,
            )
        val next =
            liveResource.put(
                "network",
                "stage",
                "resourcepacks",
                "global",
                PutDocumentBody("{}"),
                forge,
                "\"4\"",
            )

        assertEquals(11, (first.entity as WriteResultResponse).version)
        assertEquals("\"4\"", first.getHeaderString("ETag"))
        assertEquals(12, (next.entity as WriteResultResponse).version)
        assertEquals("\"5\"", next.getHeaderString("ETag"))
        verify(repository).upsertAndIncrementVersion(any(), eq(3L))
        verify(repository).upsertAndIncrementVersion(any(), eq(4L))
        verify(publisher).publishChange("network", "stage", 11, "resourcepacks", "global")
        verify(publisher).publishChange("network", "stage", 12, "resourcepacks", "global")
    }

    private fun headers(vararg entries: Pair<String, String>): HttpHeaders =
        HttpHeadersImpl(entries.map { SimpleImmutableEntry(it.first, it.second) })

    private fun security(subject: String): SecurityContext =
        mock<SecurityContext>().also { security ->
            whenever(security.userPrincipal).thenReturn(Principal { subject })
        }
}
