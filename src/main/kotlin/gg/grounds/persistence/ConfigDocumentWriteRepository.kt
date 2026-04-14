package gg.grounds.persistence

import gg.grounds.domain.ConfigDocument
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigDocumentWriteRepository @Inject constructor(private val dataSource: DataSource) {
    fun upsert(document: ConfigDocument): Boolean {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(UPSERT).use { statement ->
                    statement.setString(1, document.app)
                    statement.setString(2, document.env)
                    statement.setString(3, document.namespace)
                    statement.setString(4, document.configKey)
                    statement.setString(5, document.contentJson)
                    statement.setString(6, document.updatedBy)
                    statement.executeUpdate() > 0
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                "Failed to upsert config document (app=%s, env=%s, namespace=%s, configKey=%s, reason=%s)",
                document.app,
                document.env,
                document.namespace,
                document.configKey,
                errorReason(error),
            )
            false
        }
    }

    fun upsertAndIncrementVersion(
        document: ConfigDocument,
        expectedVersion: Long? = null,
    ): ConfigDocumentRepository.UpsertAndIncrementVersionResult {
        return try {
            dataSource.connection.use { connection ->
                val originalAutoCommit = connection.autoCommit
                connection.autoCommit = false
                try {
                    val upsertedRows = upsertDocument(connection, document, expectedVersion)
                    if (upsertedRows == 0) {
                        val currentDocumentVersion =
                            getDocumentVersion(
                                connection,
                                document.app,
                                document.env,
                                document.namespace,
                                document.configKey,
                            )
                        connection.rollback()
                        if (expectedVersion != null) {
                            ConfigDocumentRepository.UpsertAndIncrementVersionResult
                                .PreconditionFailed(currentDocumentVersion)
                        } else {
                            val error =
                                SQLException(
                                    "Failed to upsert config document (reason=no_rows_affected, app=${document.app}, env=${document.env}, namespace=${document.namespace}, configKey=${document.configKey})"
                                )
                            ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed(error)
                        }
                    } else {
                        val version = incrementVersion(connection, document.app, document.env)
                        connection.commit()
                        ConfigDocumentRepository.UpsertAndIncrementVersionResult.Updated(version)
                    }
                } catch (error: SQLException) {
                    rollbackSafely(connection, error)
                    ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed(error)
                } finally {
                    connection.autoCommit = originalAutoCommit
                }
            }
        } catch (error: SQLException) {
            ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed(error)
        }
    }

    fun insertIfNotExists(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
        defaultContentJson: String,
    ): Boolean {
        return insertIfNotExists(
                app,
                env,
                listOf(
                    ConfigDocumentRepository.DefaultConfig(namespace, configKey, defaultContentJson)
                ),
            )
            .isNotEmpty()
    }

    fun insertIfNotExists(
        app: String,
        env: String,
        defaults: List<ConfigDocumentRepository.DefaultConfig>,
    ): List<ConfigDocumentRepository.DefaultConfig> {
        if (defaults.isEmpty()) {
            return emptyList()
        }
        return try {
            dataSource.connection.use { connection ->
                val originalAutoCommit = connection.autoCommit
                connection.autoCommit = false
                try {
                    connection.prepareStatement(INSERT_IF_NOT_EXISTS).use { statement ->
                        val createdDefaults =
                            mutableListOf<ConfigDocumentRepository.DefaultConfig>()
                        for (defaultConfig in defaults) {
                            try {
                                statement.setString(1, app)
                                statement.setString(2, env)
                                statement.setString(3, defaultConfig.namespace)
                                statement.setString(4, defaultConfig.configKey)
                                statement.setString(5, defaultConfig.defaultContentJson)
                                if (statement.executeUpdate() > 0) {
                                    createdDefaults.add(defaultConfig)
                                }
                            } catch (error: SQLException) {
                                throw SQLException(
                                    "Failed to insert default config (app=$app, env=$env, namespace=${defaultConfig.namespace}, configKey=${defaultConfig.configKey})",
                                    error,
                                )
                            }
                        }
                        connection.commit()
                        createdDefaults
                    }
                } catch (error: SQLException) {
                    rollbackSafely(connection, error)
                    throw error
                } finally {
                    connection.autoCommit = originalAutoCommit
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                "Failed to insert default configs (app=%s, env=%s, count=%d, reason=%s)",
                app,
                env,
                defaults.size,
                errorReason(error),
            )
            throw error
        }
    }

    fun syncDefaults(
        app: String,
        env: String,
        defaults: List<ConfigDocumentRepository.DefaultConfig>,
    ): ConfigDocumentRepository.SyncDefaultsResult {
        if (defaults.isEmpty()) {
            return ConfigDocumentRepository.SyncDefaultsResult(emptyList(), getVersion(app, env))
        }
        return try {
            dataSource.connection.use { connection ->
                val originalAutoCommit = connection.autoCommit
                connection.autoCommit = false
                try {
                    val createdDefaults =
                        connection.prepareStatement(INSERT_IF_NOT_EXISTS).use { statement ->
                            val insertedDefaults =
                                mutableListOf<ConfigDocumentRepository.DefaultConfig>()
                            for (defaultConfig in defaults) {
                                try {
                                    statement.setString(1, app)
                                    statement.setString(2, env)
                                    statement.setString(3, defaultConfig.namespace)
                                    statement.setString(4, defaultConfig.configKey)
                                    statement.setString(5, defaultConfig.defaultContentJson)
                                    if (statement.executeUpdate() > 0) {
                                        insertedDefaults.add(defaultConfig)
                                    }
                                } catch (error: SQLException) {
                                    throw SQLException(
                                        "Failed to insert default config (app=$app, env=$env, namespace=${defaultConfig.namespace}, configKey=${defaultConfig.configKey})",
                                        error,
                                    )
                                }
                            }
                            insertedDefaults
                        }
                    val version =
                        if (createdDefaults.isNotEmpty()) {
                            incrementVersion(connection, app, env)
                        } else {
                            getVersion(connection, app, env)
                        }
                    connection.commit()
                    ConfigDocumentRepository.SyncDefaultsResult(createdDefaults, version)
                } catch (error: SQLException) {
                    rollbackSafely(connection, error)
                    throw error
                } finally {
                    connection.autoCommit = originalAutoCommit
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                "Failed to sync default configs (app=%s, env=%s, count=%d, reason=%s)",
                app,
                env,
                defaults.size,
                errorReason(error),
            )
            throw error
        }
    }

    fun delete(app: String, env: String, namespace: String, configKey: String): Boolean {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(DELETE_ONE).use { statement ->
                    statement.setString(1, app)
                    statement.setString(2, env)
                    statement.setString(3, namespace)
                    statement.setString(4, configKey)
                    statement.executeUpdate() > 0
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                "Failed to delete config document (app=%s, env=%s, namespace=%s, configKey=%s, reason=%s)",
                app,
                env,
                namespace,
                configKey,
                errorReason(error),
            )
            false
        }
    }

    fun deleteAndIncrementVersion(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
    ): ConfigDocumentRepository.DeleteAndIncrementVersionResult {
        return try {
            dataSource.connection.use { connection ->
                val originalAutoCommit = connection.autoCommit
                connection.autoCommit = false
                try {
                    val deletedRows =
                        connection.prepareStatement(DELETE_ONE).use { statement ->
                            statement.setString(1, app)
                            statement.setString(2, env)
                            statement.setString(3, namespace)
                            statement.setString(4, configKey)
                            statement.executeUpdate()
                        }
                    if (deletedRows == 0) {
                        val version = getVersion(connection, app, env)
                        connection.rollback()
                        ConfigDocumentRepository.DeleteAndIncrementVersionResult.NotFound(version)
                    } else {
                        val version = incrementVersion(connection, app, env)
                        connection.commit()
                        ConfigDocumentRepository.DeleteAndIncrementVersionResult.Deleted(version)
                    }
                } catch (error: SQLException) {
                    rollbackSafely(connection, error)
                    ConfigDocumentRepository.DeleteAndIncrementVersionResult.Failed(error)
                } finally {
                    connection.autoCommit = originalAutoCommit
                }
            }
        } catch (error: SQLException) {
            ConfigDocumentRepository.DeleteAndIncrementVersionResult.Failed(error)
        }
    }

    private fun incrementVersion(connection: Connection, app: String, env: String): Long {
        return connection.prepareStatement(INCREMENT_VERSION).use { statement ->
            statement.setString(1, app)
            statement.setString(2, env)
            statement.executeQuery().use { resultSet ->
                if (resultSet.next()) {
                    resultSet.getLong("version")
                } else {
                    throw SQLException(
                        "Failed to increment config version (reason=missing_version_row, app=$app, env=$env)"
                    )
                }
            }
        }
    }

    private fun getVersion(app: String, env: String): Long {
        return dataSource.connection.use { connection -> getVersion(connection, app, env) }
    }

    private fun getVersion(connection: Connection, app: String, env: String): Long {
        return connection.prepareStatement(SELECT_VERSION).use { statement ->
            statement.setString(1, app)
            statement.setString(2, env)
            statement.executeQuery().use { resultSet ->
                if (resultSet.next()) resultSet.getLong("version") else 0L
            }
        }
    }

    private fun rollbackSafely(connection: Connection, originalError: SQLException) {
        try {
            connection.rollback()
        } catch (rollbackError: SQLException) {
            originalError.addSuppressed(rollbackError)
        }
    }

    private fun upsertDocument(
        connection: Connection,
        document: ConfigDocument,
        expectedVersion: Long?,
    ): Int {
        return if (expectedVersion != null) {
            connection.prepareStatement(UPDATE_IF_VERSION_MATCHES).use { statement ->
                statement.setString(1, document.contentJson)
                statement.setString(2, document.updatedBy)
                statement.setString(3, document.app)
                statement.setString(4, document.env)
                statement.setString(5, document.namespace)
                statement.setString(6, document.configKey)
                statement.setLong(7, expectedVersion)
                statement.executeUpdate()
            }
        } else {
            connection.prepareStatement(UPSERT).use { statement ->
                statement.setString(1, document.app)
                statement.setString(2, document.env)
                statement.setString(3, document.namespace)
                statement.setString(4, document.configKey)
                statement.setString(5, document.contentJson)
                statement.setString(6, document.updatedBy)
                statement.executeUpdate()
            }
        }
    }

    private fun getDocumentVersion(
        connection: Connection,
        app: String,
        env: String,
        namespace: String,
        configKey: String,
    ): Long? {
        return connection.prepareStatement(SELECT_DOCUMENT_VERSION).use { statement ->
            statement.setString(1, app)
            statement.setString(2, env)
            statement.setString(3, namespace)
            statement.setString(4, configKey)
            statement.executeQuery().use { resultSet ->
                if (resultSet.next()) {
                    resultSet.getLong("version")
                } else {
                    null
                }
            }
        }
    }

    private fun errorReason(error: SQLException): String {
        return error.message ?: error::class.java.simpleName
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigDocumentWriteRepository::class.java)

        private const val UPSERT =
            """
            INSERT INTO config_documents (app, env, namespace, config_key, content, version, updated_by, updated_at)
            VALUES (?, ?, ?, ?, ?::jsonb, 1, ?, now())
            ON CONFLICT (app, env, namespace, config_key)
            DO UPDATE SET content = EXCLUDED.content,
                          version = config_documents.version + 1,
                          updated_by = EXCLUDED.updated_by,
                          updated_at = now()
            """

        private const val UPDATE_IF_VERSION_MATCHES =
            """
            UPDATE config_documents
            SET content = ?::jsonb,
                version = version + 1,
                updated_by = ?,
                updated_at = now()
            WHERE app = ? AND env = ? AND namespace = ? AND config_key = ? AND version = ?
            """

        private const val SELECT_DOCUMENT_VERSION =
            """
            SELECT version
            FROM config_documents
            WHERE app = ? AND env = ? AND namespace = ? AND config_key = ?
            """

        private const val INSERT_IF_NOT_EXISTS =
            """
            INSERT INTO config_documents (app, env, namespace, config_key, content)
            VALUES (?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (app, env, namespace, config_key) DO NOTHING
            """

        private const val DELETE_ONE =
            """
            DELETE FROM config_documents
            WHERE app = ? AND env = ? AND namespace = ? AND config_key = ?
            """

        private const val INCREMENT_VERSION =
            """
            INSERT INTO config_app_versions (app, env, version, updated_at)
            VALUES (?, ?, 1, now())
            ON CONFLICT (app, env)
            DO UPDATE SET version = config_app_versions.version + 1,
                          updated_at = now()
            RETURNING version
            """

        private const val SELECT_VERSION =
            """
            SELECT version
            FROM config_app_versions
            WHERE app = ? AND env = ?
            """
    }
}
