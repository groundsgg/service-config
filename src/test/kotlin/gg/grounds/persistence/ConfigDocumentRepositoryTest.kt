package gg.grounds.persistence

import gg.grounds.domain.ConfigDocument
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
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
    private val repository = createRepository(dataSource)

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
    fun `getSnapshot reads version and documents in one repeatable read transaction`() {
        val snapshotDataSource: DataSource = mock()
        val snapshotConnection: Connection = mock()
        val versionStatement: PreparedStatement = mock()
        val documentStatement: PreparedStatement = mock()
        val versionResultSet: ResultSet = mock()
        val documentResultSet: ResultSet = mock()
        val snapshotRepository = createRepository(snapshotDataSource)
        whenever(snapshotDataSource.connection).thenReturn(snapshotConnection)
        whenever(snapshotConnection.autoCommit).thenReturn(true)
        whenever(snapshotConnection.isReadOnly).thenReturn(false)
        whenever(snapshotConnection.transactionIsolation)
            .thenReturn(Connection.TRANSACTION_READ_COMMITTED)
        whenever(snapshotConnection.prepareStatement(any()))
            .thenReturn(versionStatement, documentStatement)
        whenever(versionStatement.executeQuery()).thenReturn(versionResultSet)
        whenever(versionResultSet.next()).thenReturn(true)
        whenever(versionResultSet.getLong("version")).thenReturn(7L)
        whenever(documentStatement.executeQuery()).thenReturn(documentResultSet)
        whenever(documentResultSet.next()).thenReturn(true, false)
        whenever(documentResultSet.getLong("id")).thenReturn(1L)
        whenever(documentResultSet.getString("app")).thenReturn("player")
        whenever(documentResultSet.getString("env")).thenReturn("prod")
        whenever(documentResultSet.getString("namespace")).thenReturn("feature-flags")
        whenever(documentResultSet.getString("config_key")).thenReturn("new-ui")
        whenever(documentResultSet.getString("content")).thenReturn("""{"enabled":true}""")
        whenever(documentResultSet.getLong("version")).thenReturn(3L)
        whenever(documentResultSet.getTimestamp("created_at"))
            .thenReturn(Timestamp.from(Instant.parse("2026-04-11T10:15:30Z")))
        whenever(documentResultSet.getTimestamp("updated_at"))
            .thenReturn(Timestamp.from(Instant.parse("2026-04-11T10:16:30Z")))
        whenever(documentResultSet.getString("updated_by")).thenReturn("tester")

        val snapshot = snapshotRepository.getSnapshot("player", "prod")

        assertEquals(7L, snapshot.version)
        assertEquals(1, snapshot.documents.size)
        assertEquals("feature-flags", snapshot.documents.single().namespace)
        assertEquals("new-ui", snapshot.documents.single().configKey)
        assertEquals(3L, snapshot.documents.single().version)
        verify(snapshotConnection).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
        verify(snapshotConnection).setReadOnly(true)
        verify(snapshotConnection).setAutoCommit(false)
        verify(snapshotConnection).commit()
    }

    @Test
    fun `getSnapshotIfNewer skips document query when version is unchanged`() {
        val snapshotDataSource: DataSource = mock()
        val snapshotConnection: Connection = mock()
        val versionStatement: PreparedStatement = mock()
        val versionResultSet: ResultSet = mock()
        val snapshotRepository = createRepository(snapshotDataSource)
        whenever(snapshotDataSource.connection).thenReturn(snapshotConnection)
        whenever(snapshotConnection.autoCommit).thenReturn(true)
        whenever(snapshotConnection.isReadOnly).thenReturn(false)
        whenever(snapshotConnection.transactionIsolation)
            .thenReturn(Connection.TRANSACTION_READ_COMMITTED)
        whenever(snapshotConnection.prepareStatement(any())).thenReturn(versionStatement)
        whenever(versionStatement.executeQuery()).thenReturn(versionResultSet)
        whenever(versionResultSet.next()).thenReturn(true)
        whenever(versionResultSet.getLong("version")).thenReturn(7L)

        val snapshot = snapshotRepository.getSnapshotIfNewer("player", "prod", 7L)

        assertNull(snapshot)
        verify(snapshotConnection).commit()
        verify(snapshotConnection, times(1)).prepareStatement(any())
    }

    @Test
    fun `upsertAndIncrementVersion commits and returns updated version when successful`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val upsertStatement: PreparedStatement = mock()
        val incrementStatement: PreparedStatement = mock()
        val incrementResultSet: ResultSet = mock()
        val transactionRepository = createRepository(transactionDataSource)
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
        val transactionRepository = createRepository(transactionDataSource)
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
    fun `upsertAndIncrementVersion returns precondition failed when expected version does not match`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val updateStatement: PreparedStatement = mock()
        val currentVersionStatement: PreparedStatement = mock()
        val currentVersionResultSet: ResultSet = mock()
        val transactionRepository = createRepository(transactionDataSource)
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
            .thenReturn(updateStatement, currentVersionStatement)
        whenever(updateStatement.executeUpdate()).thenReturn(0)
        whenever(currentVersionStatement.executeQuery()).thenReturn(currentVersionResultSet)
        whenever(currentVersionResultSet.next()).thenReturn(true)
        whenever(currentVersionResultSet.getLong("version")).thenReturn(4L)

        val result = transactionRepository.upsertAndIncrementVersion(document, 3L)

        assertEquals(
            ConfigDocumentRepository.UpsertAndIncrementVersionResult.PreconditionFailed(4L),
            result,
        )
        verify(transactionConnection).rollback()
        verify(transactionConnection, never()).commit()
    }

    @Test
    fun `createAndIncrementVersion commits and returns created version when document is new`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val insertStatement: PreparedStatement = mock()
        val incrementStatement: PreparedStatement = mock()
        val incrementResultSet: ResultSet = mock()
        val transactionRepository = createRepository(transactionDataSource)
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
            .thenReturn(insertStatement, incrementStatement)
        whenever(insertStatement.executeUpdate()).thenReturn(1)
        whenever(incrementStatement.executeQuery()).thenReturn(incrementResultSet)
        whenever(incrementResultSet.next()).thenReturn(true)
        whenever(incrementResultSet.getLong("version")).thenReturn(9L)

        val result = transactionRepository.createAndIncrementVersion(document)

        assertEquals(ConfigDocumentRepository.CreateAndIncrementVersionResult.Created(9L), result)
        verify(transactionConnection).commit()
        verify(transactionConnection, never()).rollback()
    }

    @Test
    fun `createAndIncrementVersion returns already exists when document already exists`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val insertStatement: PreparedStatement = mock()
        val currentVersionStatement: PreparedStatement = mock()
        val currentVersionResultSet: ResultSet = mock()
        val transactionRepository = createRepository(transactionDataSource)
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
            .thenReturn(insertStatement, currentVersionStatement)
        whenever(insertStatement.executeUpdate()).thenReturn(0)
        whenever(currentVersionStatement.executeQuery()).thenReturn(currentVersionResultSet)
        whenever(currentVersionResultSet.next()).thenReturn(true)
        whenever(currentVersionResultSet.getLong("version")).thenReturn(3L)

        val result = transactionRepository.createAndIncrementVersion(document)

        assertEquals(
            ConfigDocumentRepository.CreateAndIncrementVersionResult.AlreadyExists(3L),
            result,
        )
        verify(transactionConnection).rollback()
        verify(transactionConnection, never()).commit()
    }

    @Test
    fun `upsertAndIncrementVersion returns not found when expected version is provided and document is missing`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val updateStatement: PreparedStatement = mock()
        val currentVersionStatement: PreparedStatement = mock()
        val currentVersionResultSet: ResultSet = mock()
        val transactionRepository = createRepository(transactionDataSource)
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
            .thenReturn(updateStatement, currentVersionStatement)
        whenever(updateStatement.executeUpdate()).thenReturn(0)
        whenever(currentVersionStatement.executeQuery()).thenReturn(currentVersionResultSet)
        whenever(currentVersionResultSet.next()).thenReturn(false)

        val result = transactionRepository.upsertAndIncrementVersion(document, 3L)

        assertEquals(ConfigDocumentRepository.UpsertAndIncrementVersionResult.NotFound, result)
        verify(transactionConnection).rollback()
        verify(transactionConnection, never()).commit()
    }

    @Test
    fun `insertIfNotExists with defaults list reuses connection and statement`() {
        val batchDataSource: DataSource = mock()
        val batchConnection: Connection = mock()
        val batchStatement: PreparedStatement = mock()
        val batchRepository = createRepository(batchDataSource)
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
        val batchRepository = createRepository(batchDataSource)
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

    @Test
    fun `syncDefaults commits inserts and version increment in one transaction`() {
        val batchDataSource: DataSource = mock()
        val batchConnection: Connection = mock()
        val insertStatement: PreparedStatement = mock()
        val incrementStatement: PreparedStatement = mock()
        val incrementResultSet: ResultSet = mock()
        val batchRepository = createRepository(batchDataSource)
        val defaultConfig =
            ConfigDocumentRepository.DefaultConfig(
                namespace = "feature-flags",
                configKey = "new-ui",
                defaultContentJson = "{}",
            )
        whenever(batchDataSource.connection).thenReturn(batchConnection)
        whenever(batchConnection.autoCommit).thenReturn(true)
        whenever(batchConnection.prepareStatement(any()))
            .thenReturn(insertStatement, incrementStatement)
        whenever(insertStatement.executeUpdate()).thenReturn(1)
        whenever(incrementStatement.executeQuery()).thenReturn(incrementResultSet)
        whenever(incrementResultSet.next()).thenReturn(true)
        whenever(incrementResultSet.getLong("version")).thenReturn(7L)

        val result = batchRepository.syncDefaults("player", "prod", listOf(defaultConfig))

        assertEquals(listOf(defaultConfig), result.createdDefaults)
        assertEquals(7L, result.version)
        verify(batchConnection).commit()
        verify(batchConnection, never()).rollback()
    }

    @Test
    fun `syncDefaults rolls back when version increment fails`() {
        val batchDataSource: DataSource = mock()
        val batchConnection: Connection = mock()
        val insertStatement: PreparedStatement = mock()
        val incrementStatement: PreparedStatement = mock()
        val batchRepository = createRepository(batchDataSource)
        val defaultConfig =
            ConfigDocumentRepository.DefaultConfig(
                namespace = "feature-flags",
                configKey = "new-ui",
                defaultContentJson = "{}",
            )
        val sqlError = SQLException("increment failed")
        whenever(batchDataSource.connection).thenReturn(batchConnection)
        whenever(batchConnection.autoCommit).thenReturn(true)
        whenever(batchConnection.prepareStatement(any()))
            .thenReturn(insertStatement, incrementStatement)
        whenever(insertStatement.executeUpdate()).thenReturn(1)
        whenever(incrementStatement.executeQuery()).thenThrow(sqlError)

        val thrown =
            assertThrows(SQLException::class.java) {
                batchRepository.syncDefaults("player", "prod", listOf(defaultConfig))
            }

        assertEquals(sqlError, thrown)
        verify(batchConnection).rollback()
        verify(batchConnection, never()).commit()
    }

    @Test
    fun `deleteAndIncrementVersion returns not found with current version when document is missing`() {
        val transactionDataSource: DataSource = mock()
        val transactionConnection: Connection = mock()
        val deleteStatement: PreparedStatement = mock()
        val versionStatement: PreparedStatement = mock()
        val versionResultSet: ResultSet = mock()
        val transactionRepository = createRepository(transactionDataSource)
        whenever(transactionDataSource.connection).thenReturn(transactionConnection)
        whenever(transactionConnection.autoCommit).thenReturn(true)
        whenever(transactionConnection.prepareStatement(any()))
            .thenReturn(deleteStatement, versionStatement)
        whenever(deleteStatement.executeUpdate()).thenReturn(0)
        whenever(versionStatement.executeQuery()).thenReturn(versionResultSet)
        whenever(versionResultSet.next()).thenReturn(true)
        whenever(versionResultSet.getLong("version")).thenReturn(11L)

        val result =
            transactionRepository.deleteAndIncrementVersion(
                "player",
                "prod",
                "feature-flags",
                "new-ui",
            )

        assertEquals(ConfigDocumentRepository.DeleteAndIncrementVersionResult.NotFound(11L), result)
        verify(transactionConnection).rollback()
        verify(transactionConnection, never()).commit()
    }

    private fun createRepository(dataSource: DataSource) =
        ConfigDocumentRepository(
            dataSource,
            ConfigDocumentReadRepository(dataSource),
            ConfigDocumentWriteRepository(dataSource),
            ConfigVersionRepository(dataSource),
        )
}
