package gg.grounds.api

import com.fasterxml.jackson.databind.ObjectMapper
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.DeleteDocumentRequest
import gg.grounds.grpc.config.PutDocumentRequest
import gg.grounds.persistence.ConfigDocumentRepository
import io.grpc.Status
import io.grpc.StatusRuntimeException
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoInteractions

class ConfigAdminDocumentServiceTest {
    private val dataSource: DataSource = mock()
    private val repository = ConfigDocumentRepository(dataSource)
    private val objectMapper = ObjectMapper()
    private val changePublisher = ConfigChangePublisher("nats://localhost:4222", objectMapper)
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
        verifyNoInteractions(dataSource)
    }

    @Test
    fun `deleteDocument returns failed precondition because delete is not supported`() {
        val request =
            DeleteDocumentRequest.newBuilder()
                .setApp("player")
                .setEnv("prod")
                .setNamespace("feature-flags")
                .setConfigKey("new-ui")
                .build()

        val thrown =
            assertThrows(StatusRuntimeException::class.java) { service.deleteDocument(request) }

        assertEquals(Status.Code.FAILED_PRECONDITION, thrown.status.code)
        assertEquals(
            "DeleteDocument is not supported because config documents are the persisted runtime truth; use PutDocument to replace the value",
            thrown.status.description,
        )
        verifyNoInteractions(dataSource)
    }
}
