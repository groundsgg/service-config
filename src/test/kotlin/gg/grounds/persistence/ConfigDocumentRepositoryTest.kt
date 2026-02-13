package gg.grounds.persistence

import gg.grounds.domain.ConfigDocument
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class ConfigDocumentRepositoryTest {
    private val dataSource: DataSource = mock()
    private val repository = ConfigDocumentRepository(dataSource)

    @Test
    fun `findAll rethrows SQLException when read fails`() {
        val sqlError = SQLException("database unavailable")
        whenever(dataSource.connection).thenThrow(sqlError)

        val thrown = assertThrows(SQLException::class.java) { repository.findAll("player", "prod") }

        assertEquals(sqlError, thrown)
    }

    @Test
    fun `findByNamespace rethrows SQLException when read fails`() {
        val sqlError = SQLException("database unavailable")
        whenever(dataSource.connection).thenThrow(sqlError)

        val thrown =
            assertThrows(SQLException::class.java) {
                repository.findByNamespace("player", "prod", "feature-flags")
            }

        assertEquals(sqlError, thrown)
    }

    @Test
    fun `findOne rethrows SQLException when read fails`() {
        val sqlError = SQLException("database unavailable")
        whenever(dataSource.connection).thenThrow(sqlError)

        val thrown =
            assertThrows(SQLException::class.java) {
                repository.findOne("player", "prod", "feature-flags", "new-ui")
            }

        assertEquals(sqlError, thrown)
    }

    @Test
    fun `upsertAndIncrementVersion commits and returns updated version when successful`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val upsertStatement: PreparedStatement = mock()
        val incrementStatement: PreparedStatement = mock()
        val incrementResultSet: ResultSet = mock()
        val transactionRepository = ConfigDocumentRepository(transactionDataSource)
        val document =
            ConfigDocument(
                app = "player",
                env = "prod",
                namespace = "feature-flags",
                configKey = "new-ui",
                contentJson = "{}",
                updatedBy = "tester",
            )
        whenever(transactionDataSource.connection).thenReturn(transactionConnection)
        whenever(transactionConnection.autoCommit).thenReturn(true)
        whenever(transactionConnection.prepareStatement(any()))
            .thenReturn(upsertStatement, incrementStatement)
        whenever(upsertStatement.executeUpdate()).thenReturn(1)
        whenever(incrementStatement.executeQuery()).thenReturn(incrementResultSet)
        whenever(incrementResultSet.next()).thenReturn(true)
        whenever(incrementResultSet.getLong("version")).thenReturn(42L)

        val result = transactionRepository.upsertAndIncrementVersion(document)

        assertEquals(ConfigDocumentRepository.UpsertAndIncrementVersionResult.Updated(42L), result)
        verify(transactionConnection).commit()
    }

    @Test
    fun `upsertAndIncrementVersion rolls back and returns failed when increment fails`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val upsertStatement: PreparedStatement = mock()
        val incrementStatement: PreparedStatement = mock()
        val transactionRepository = ConfigDocumentRepository(transactionDataSource)
        val document =
            ConfigDocument(
                app = "player",
                env = "prod",
                namespace = "feature-flags",
                configKey = "new-ui",
                contentJson = "{}",
                updatedBy = "tester",
            )
        val sqlError = SQLException("increment failed")
        whenever(transactionDataSource.connection).thenReturn(transactionConnection)
        whenever(transactionConnection.autoCommit).thenReturn(true)
        whenever(transactionConnection.prepareStatement(any()))
            .thenReturn(upsertStatement, incrementStatement)
        whenever(upsertStatement.executeUpdate()).thenReturn(1)
        whenever(incrementStatement.executeQuery()).thenThrow(sqlError)

        val result = transactionRepository.upsertAndIncrementVersion(document)

        assertTrue(result is ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed)
        assertEquals(
            sqlError,
            (result as ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed).cause,
        )
        verify(transactionConnection).rollback()
        verify(transactionConnection, never()).commit()
    }

    @Test
    fun `insertIfNotExists with defaults list reuses connection and statement`() {
        val batchDataSource: DataSource = mock()
        val batchConnection: Connection = mock()
        val batchStatement: PreparedStatement = mock()
        val batchRepository = ConfigDocumentRepository(batchDataSource)
        val firstDefault =
            ConfigDocumentRepository.DefaultConfig(
                namespace = "feature-flags",
                configKey = "new-ui",
                defaultContentJson = "{}",
            )
        val secondDefault =
            ConfigDocumentRepository.DefaultConfig(
                namespace = "feature-flags",
                configKey = "legacy-mode",
                defaultContentJson = "{}",
            )
        whenever(batchDataSource.connection).thenReturn(batchConnection)
        whenever(batchConnection.autoCommit).thenReturn(true)
        whenever(batchConnection.prepareStatement(any())).thenReturn(batchStatement)
        whenever(batchStatement.executeUpdate()).thenReturn(1, 0)

        val created =
            batchRepository.insertIfNotExists("player", "prod", listOf(firstDefault, secondDefault))

        assertEquals(listOf(firstDefault), created)
        verify(batchDataSource, times(1)).connection
        verify(batchConnection, times(1)).prepareStatement(any())
        verify(batchStatement, times(2)).executeUpdate()
        verify(batchConnection).commit()
    }

    @Test
    fun `insertIfNotExists with defaults list rolls back when insert fails`() {
        val batchDataSource: DataSource = mock()
        val batchConnection: Connection = mock()
        val batchStatement: PreparedStatement = mock()
        val batchRepository = ConfigDocumentRepository(batchDataSource)
        val firstDefault =
            ConfigDocumentRepository.DefaultConfig(
                namespace = "feature-flags",
                configKey = "new-ui",
                defaultContentJson = "{}",
            )
        val secondDefault =
            ConfigDocumentRepository.DefaultConfig(
                namespace = "feature-flags",
                configKey = "legacy-mode",
                defaultContentJson = "{}",
            )
        val sqlError = SQLException("insert failed")
        whenever(batchDataSource.connection).thenReturn(batchConnection)
        whenever(batchConnection.autoCommit).thenReturn(true)
        whenever(batchConnection.prepareStatement(any())).thenReturn(batchStatement)
        whenever(batchStatement.executeUpdate()).thenReturn(1).thenThrow(sqlError)

        val thrown =
            assertThrows(SQLException::class.java) {
                batchRepository.insertIfNotExists(
                    "player",
                    "prod",
                    listOf(firstDefault, secondDefault),
                )
            }

        assertTrue(thrown.message!!.contains("Failed to insert default config"))
        verify(batchConnection).rollback()
        verify(batchConnection, never()).commit()
    }
}
