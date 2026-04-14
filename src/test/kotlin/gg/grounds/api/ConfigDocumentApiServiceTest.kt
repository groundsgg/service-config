package gg.grounds.api

import com.fasterxml.jackson.databind.ObjectMapper
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.GetSnapshotIfNewerRequest
import gg.grounds.persistence.ConfigDocumentRepository
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ConfigDocumentApiServiceTest {
    private val repository: ConfigDocumentRepository = mock()
    private val changePublisher: ConfigChangePublisher = mock()
    private val service = ConfigDocumentApiService(repository, changePublisher, ObjectMapper())

    @Test
    fun `getSnapshotIfNewer returns current server version when snapshot is unchanged`() {
        whenever(repository.getSnapshotIfNewer("player", "prod", 7L)).thenReturn(null)
        whenever(repository.getVersion("player", "prod")).thenReturn(9L)

        val response =
            service.getSnapshotIfNewer(
                GetSnapshotIfNewerRequest.newBuilder()
                    .setApp("player")
                    .setEnv("prod")
                    .setKnownVersion(7L)
                    .build()
            )

        assertEquals(false, response.changed)
        assertEquals(9L, response.version)
        assertEquals(0, response.documentsCount)
    }
}
