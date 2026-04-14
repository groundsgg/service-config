package gg.grounds.api

import gg.grounds.domain.ConfigDocument
import gg.grounds.persistence.ConfigDocumentRepository
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ConfigDocumentLookupTest {
    private val repository: ConfigDocumentRepository = mock()

    @Test
    fun `getDocumentResponse returns not found when document does not exist`() {
        whenever(repository.findOne("player", "prod", "feature-flags", "new-ui")).thenReturn(null)

        val thrown =
            assertThrows(StatusRuntimeException::class.java) {
                ConfigDocumentLookup.getDocumentResponse(
                    repository,
                    ConfigRequestContexts.toDocumentContext(
                        "player",
                        "prod",
                        "feature-flags",
                        "new-ui",
                    ),
                )
            }

        assertEquals(Status.Code.NOT_FOUND, thrown.status.code)
        assertEquals(
            "Config document not found (app=player, env=prod, namespace=feature-flags, configKey=new-ui)",
            thrown.status.description,
        )
    }

    @Test
    fun `getDocumentResponse includes document version`() {
        whenever(repository.findOne("player", "prod", "feature-flags", "new-ui"))
            .thenReturn(
                ConfigDocument(
                    app = "player",
                    env = "prod",
                    namespace = "feature-flags",
                    configKey = "new-ui",
                    contentJson = """{"enabled":true}""",
                    version = 6,
                )
            )

        val response =
            ConfigDocumentLookup.getDocumentResponse(
                repository,
                ConfigRequestContexts.toDocumentContext("player", "prod", "feature-flags", "new-ui"),
            )

        assertEquals(6L, response.document.version)
    }
}
