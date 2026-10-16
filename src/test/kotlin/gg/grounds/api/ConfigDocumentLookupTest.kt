package gg.grounds.api

import gg.grounds.domain.ConfigDocument
import gg.grounds.domain.ConfigErrorCode
import gg.grounds.domain.ConfigException
import gg.grounds.persistence.ConfigDocumentRepository
import java.time.Instant
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

    @Test
    fun `findDocument returns the domain document including its last write`() {
        val expectedUpdatedAt = Instant.parse("2026-10-16T15:46:24Z")
        whenever(repository.findOne("network", "stage", "resourcepacks", "global"))
            .thenReturn(
                ConfigDocument(
                    app = "network",
                    env = "stage",
                    namespace = "resourcepacks",
                    configKey = "global",
                    contentJson = "{}",
                    version = 14,
                    updatedAt = expectedUpdatedAt,
                    updatedBy = "keycloak-sub",
                )
            )

        val actualDocument =
            ConfigDocumentLookup.findDocument(
                repository,
                ConfigRequestContexts.toDocumentContext("network", "stage", "resourcepacks", "global"),
            )

        assertEquals(14L, actualDocument.version)
        assertEquals(expectedUpdatedAt, actualDocument.updatedAt)
        assertEquals("keycloak-sub", actualDocument.updatedBy)
    }

    @Test
    fun `findDocument returns not found when document does not exist`() {
        whenever(repository.findOne("network", "stage", "resourcepacks", "global")).thenReturn(null)

        val thrown =
            assertThrows(ConfigException::class.java) {
                ConfigDocumentLookup.findDocument(
                    repository,
                    ConfigRequestContexts.toDocumentContext("network", "stage", "resourcepacks", "global"),
                )
            }

        assertEquals(ConfigErrorCode.NOT_FOUND, thrown.code)
    }
}
