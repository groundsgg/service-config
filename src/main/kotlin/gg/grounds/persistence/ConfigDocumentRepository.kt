package gg.grounds.persistence

import gg.grounds.domain.ConfigDocument
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource

@ApplicationScoped
class ConfigDocumentRepository
@Inject
internal constructor(
    private val dataSource: DataSource,
    private val readRepository: ConfigDocumentReadRepository,
    private val writeRepository: ConfigDocumentWriteRepository,
    private val versionRepository: ConfigVersionRepository,
) {
    data class DefaultConfig(
        val namespace: String,
        val configKey: String,
        val defaultContentJson: String,
    )

    data class SnapshotResult(val version: Long, val documents: List<ConfigDocument>)

    data class SyncDefaultsResult(val createdDefaults: List<DefaultConfig>, val version: Long)

    sealed interface UpsertAndIncrementVersionResult {
        data class Updated(val version: Long) : UpsertAndIncrementVersionResult

        data class PreconditionFailed(val currentDocumentVersion: Long) :
            UpsertAndIncrementVersionResult

        data object NotFound : UpsertAndIncrementVersionResult

        data class Failed(val cause: SQLException) : UpsertAndIncrementVersionResult
    }

    sealed interface CreateAndIncrementVersionResult {
        data class Created(val version: Long) : CreateAndIncrementVersionResult

        data class AlreadyExists(val currentDocumentVersion: Long) : CreateAndIncrementVersionResult

        data class Failed(val cause: SQLException) : CreateAndIncrementVersionResult
    }

    sealed interface DeleteAndIncrementVersionResult {
        data class Deleted(val version: Long) : DeleteAndIncrementVersionResult

        data class NotFound(val version: Long) : DeleteAndIncrementVersionResult

        data class Failed(val cause: SQLException) : DeleteAndIncrementVersionResult
    }

    fun findAll(app: String, env: String): List<ConfigDocument> = readRepository.findAll(app, env)

    fun findByNamespace(app: String, env: String, namespace: String): List<ConfigDocument> =
        readRepository.findByNamespace(app, env, namespace)

    fun getSnapshot(app: String, env: String): SnapshotResult =
        withRepeatableReadTransaction(app, env) { connection ->
            SnapshotResult(
                version = versionRepository.getVersion(connection, app, env),
                documents = readRepository.findAll(connection, app, env),
            )
        }

    fun getSnapshotIfNewer(app: String, env: String, knownVersion: Long): SnapshotResult? =
        withRepeatableReadTransaction(app, env) { connection ->
            val currentVersion = versionRepository.getVersion(connection, app, env)
            if (currentVersion <= knownVersion) {
                null
            } else {
                SnapshotResult(
                    version = currentVersion,
                    documents = readRepository.findAll(connection, app, env),
                )
            }
        }

    fun getNamespaceSnapshot(app: String, env: String, namespace: String): SnapshotResult =
        withRepeatableReadTransaction(app, env) { connection ->
            SnapshotResult(
                version = versionRepository.getVersion(connection, app, env),
                documents = readRepository.findByNamespace(connection, app, env, namespace),
            )
        }

    fun findOne(app: String, env: String, namespace: String, configKey: String): ConfigDocument? =
        readRepository.findOne(app, env, namespace, configKey)

    fun getVersion(app: String, env: String): Long = versionRepository.getVersion(app, env)

    fun upsert(document: ConfigDocument): Boolean = writeRepository.upsert(document)

    fun upsertAndIncrementVersion(
        document: ConfigDocument,
        expectedVersion: Long? = null,
    ): UpsertAndIncrementVersionResult =
        writeRepository.upsertAndIncrementVersion(document, expectedVersion)

    fun createAndIncrementVersion(document: ConfigDocument): CreateAndIncrementVersionResult =
        writeRepository.createAndIncrementVersion(document)

    fun insertIfNotExists(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
        defaultContentJson: String,
    ): Boolean =
        writeRepository.insertIfNotExists(app, env, namespace, configKey, defaultContentJson)

    fun insertIfNotExists(
        app: String,
        env: String,
        defaults: List<DefaultConfig>,
    ): List<DefaultConfig> = writeRepository.insertIfNotExists(app, env, defaults)

    fun syncDefaults(app: String, env: String, defaults: List<DefaultConfig>): SyncDefaultsResult =
        writeRepository.syncDefaults(app, env, defaults)

    fun delete(app: String, env: String, namespace: String, configKey: String): Boolean =
        writeRepository.delete(app, env, namespace, configKey)

    fun deleteAndIncrementVersion(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
    ): DeleteAndIncrementVersionResult =
        writeRepository.deleteAndIncrementVersion(app, env, namespace, configKey)

    private fun <T> withRepeatableReadTransaction(
        app: String,
        env: String,
        block: (Connection) -> T,
    ): T {
        return try {
            dataSource.connection.use { connection ->
                val originalAutoCommit = connection.autoCommit
                val originalReadOnly = connection.isReadOnly
                val originalIsolation = connection.transactionIsolation
                connection.autoCommit = false
                connection.isReadOnly = true
                connection.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ
                try {
                    val result = block(connection)
                    connection.commit()
                    result
                } catch (error: SQLException) {
                    rollbackSafely(connection, error)
                    throw error
                } finally {
                    connection.transactionIsolation = originalIsolation
                    connection.isReadOnly = originalReadOnly
                    connection.autoCommit = originalAutoCommit
                }
            }
        } catch (error: SQLException) {
            throw SQLException("Failed to read config snapshot (app=$app, env=$env)", error)
        }
    }

    private fun rollbackSafely(connection: Connection, originalError: SQLException) {
        try {
            connection.rollback()
        } catch (rollbackError: SQLException) {
            originalError.addSuppressed(rollbackError)
        }
    }
}
