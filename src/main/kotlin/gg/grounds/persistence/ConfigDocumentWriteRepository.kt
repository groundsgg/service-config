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
                error,
                "Failed to upsert config document (app=%s, env=%s, namespace=%s, configKey=%s)",
                document.app,
                document.env,
                document.namespace,
                document.configKey,
            )
            false
        }
    }

    fun upsertAndIncrementVersion(
        document: ConfigDocument
    ): ConfigDocumentRepository.UpsertAndIncrementVersionResult {
        return try {
            dataSource.connection.use { connection ->
                val originalAutoCommit = connection.autoCommit
                connection.autoCommit = false
                try {
                    val upsertedRows =
                        connection.prepareStatement(UPSERT).use { statement ->
                            statement.setString(1, document.app)
                            statement.setString(2, document.env)
                            statement.setString(3, document.namespace)
                            statement.setString(4, document.configKey)
                            statement.setString(5, document.contentJson)
                            statement.setString(6, document.updatedBy)
                            statement.executeUpdate()
                        }
                    if (upsertedRows == 0) {
                        val error =
                            SQLException(
                                "Failed to upsert config document (reason=no_rows_affected, app=${document.app}, env=${document.env}, namespace=${document.namespace}, configKey=${document.configKey})"
                            )
                        rollbackSafely(connection, error)
                        ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed(error)
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
                error,
                "Failed to insert default configs (app=%s, env=%s, count=%d)",
                app,
                env,
                defaults.size,
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
                error,
                "Failed to delete config document (app=%s, env=%s, namespace=%s, configKey=%s)",
                app,
                env,
                namespace,
                configKey,
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
                        connection.rollback()
                        ConfigDocumentRepository.DeleteAndIncrementVersionResult.NotFound
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

    private fun rollbackSafely(connection: Connection, originalError: SQLException) {
        try {
            connection.rollback()
        } catch (rollbackError: SQLException) {
            originalError.addSuppressed(rollbackError)
        }
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigDocumentWriteRepository::class.java)

        private const val UPSERT =
            """
            INSERT INTO config_documents (app, env, namespace, config_key, content, updated_by, updated_at)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, now())
            ON CONFLICT (app, env, namespace, config_key)
            DO UPDATE SET content = EXCLUDED.content,
                          updated_by = EXCLUDED.updated_by,
                          updated_at = now()
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
    }
}
