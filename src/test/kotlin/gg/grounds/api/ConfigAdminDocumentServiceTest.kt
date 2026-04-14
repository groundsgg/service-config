package gg.grounds.api

import com.fasterxml.jackson.databind.ObjectMapper
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.DeleteDocumentRequest
import gg.grounds.grpc.config.PutDocumentRequest
import gg.grounds.persistence.ConfigDocumentRepository
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever

class ConfigAdminDocumentServiceTest {
    private val repository: ConfigDocumentRepository = mock()
    private val objectMapper = ObjectMapper()
    private val changePublisher: ConfigChangePublisher = mock()
    private val service = ConfigAdminDocumentService(repository, changePublisher, objectMapper)

    @Test
    fun `putDocument returns invalid argument when contentJson is invalid`() {
        val request =
            PutDocumentRequest.newBuilder()
                .setApp("player")
                .setEnv("prod")
                .setNamespace("feature-flags")
                .setConfigKey("new-ui")
                .setContentJson("{invalid")
                .build()

        val thrown =
            assertThrows(StatusRuntimeException::class.java) { service.putDocument(request) }

        assertEquals(Status.Code.INVALID_ARGUMENT, thrown.status.code)
        assertEquals("contentJson must be valid JSON", thrown.status.description)
        verifyNoInteractions(repository)
    }

    @Test
    fun `putDocument returns failed precondition when expected version does not match`() {
        whenever(repository.upsertAndIncrementVersion(any(), eq(3L)))
            .thenReturn(
                ConfigDocumentRepository.UpsertAndIncrementVersionResult.PreconditionFailed(4L)
            )

        val request =
            PutDocumentRequest.newBuilder()
                .setApp("player")
                .setEnv("prod")
                .setNamespace("feature-flags")
                .setConfigKey("new-ui")
                .setContentJson("{}")
                .setExpectedVersion(3L)
                .build()

        val thrown =
            assertThrows(StatusRuntimeException::class.java) { service.putDocument(request) }

        assertEquals(Status.Code.FAILED_PRECONDITION, thrown.status.code)
        assertEquals(
            "Config document version mismatch (app=player, env=prod, namespace=feature-flags, configKey=new-ui, expectedVersion=3, currentVersion=4)",
            thrown.status.description,
        )
    }

    @Test
    fun `deleteDocument returns deleted false when document does not exist`() {
        whenever(repository.deleteAndIncrementVersion("player", "prod", "feature-flags", "new-ui"))
            .thenReturn(ConfigDocumentRepository.DeleteAndIncrementVersionResult.NotFound(11L))

        val request =
            DeleteDocumentRequest.newBuilder()
                .setApp("player")
                .setEnv("prod")
                .setNamespace("feature-flags")
                .setConfigKey("new-ui")
                .setDeletedBy("tester")
                .build()

        val response = service.deleteDocument(request)

        assertEquals(false, response.deleted)
        assertEquals(11L, response.version)
    }
}
