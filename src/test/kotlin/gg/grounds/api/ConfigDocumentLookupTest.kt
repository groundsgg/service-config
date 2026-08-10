package gg.grounds.api

import gg.grounds.domain.ConfigDocument
import gg.grounds.domain.ConfigErrorCode
import gg.grounds.domain.ConfigException
import gg.grounds.persistence.ConfigDocumentRepository
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
            assertThrows(ConfigException::class.java) {
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

        assertEquals(ConfigErrorCode.NOT_FOUND, thrown.code)
        assertEquals(
            "Config document not found (app=player, env=prod, namespace=feature-flags, configKey=new-ui)",
            thrown.message,
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
