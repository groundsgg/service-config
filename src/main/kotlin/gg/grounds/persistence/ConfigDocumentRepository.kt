package gg.grounds.persistence

import gg.grounds.domain.ConfigDocument
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigDocumentRepository @Inject constructor(private val dataSource: DataSource) {
    sealed interface DeleteAndIncrementVersionResult {
        data class Deleted(val version: Long) : DeleteAndIncrementVersionResult

        data object NotFound : DeleteAndIncrementVersionResult

        data class Failed(val cause: SQLException) : DeleteAndIncrementVersionResult
    }

    fun findAll(app: String, env: String): List<ConfigDocument> {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(SELECT_ALL).use { statement ->
                    statement.setString(1, app)
                    statement.setString(2, env)
                    statement.executeQuery().use { resultSet ->
                        val documents = mutableListOf<ConfigDocument>()
                        while (resultSet.next()) {
                            documents.add(mapDocument(resultSet))
                        }
                        documents
                    }
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(error, "Failed to list config documents (app=%s, env=%s)", app, env)
            emptyList()
        }
    }

    fun findByNamespace(app: String, env: String, namespace: String): List<ConfigDocument> {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(SELECT_BY_NAMESPACE).use { statement ->
                    statement.setString(1, app)
                    statement.setString(2, env)
                    statement.setString(3, namespace)
                    statement.executeQuery().use { resultSet ->
                        val documents = mutableListOf<ConfigDocument>()
                        while (resultSet.next()) {
                            documents.add(mapDocument(resultSet))
                        }
                        documents
                    }
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                error,
                "Failed to list config documents (app=%s, env=%s, namespace=%s)",
                app,
                env,
                namespace,
            )
            emptyList()
        }
    }

    fun findOne(app: String, env: String, namespace: String, configKey: String): ConfigDocument? {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(SELECT_ONE).use { statement ->
                    statement.setString(1, app)
                    statement.setString(2, env)
                    statement.setString(3, namespace)
                    statement.setString(4, configKey)
                    statement.executeQuery().use { resultSet ->
                        if (resultSet.next()) mapDocument(resultSet) else null
                    }
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                error,
                "Failed to fetch config document (app=%s, env=%s, namespace=%s, configKey=%s)",
                app,
                env,
                namespace,
                configKey,
            )
            null
        }
    }

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

    fun insertIfNotExists(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
        defaultContentJson: String,
    ): Boolean {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(INSERT_IF_NOT_EXISTS).use { statement ->
                    statement.setString(1, app)
                    statement.setString(2, env)
                    statement.setString(3, namespace)
                    statement.setString(4, configKey)
                    statement.setString(5, defaultContentJson)
                    statement.executeUpdate() > 0
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(
                error,
                "Failed to insert default config (app=%s, env=%s, namespace=%s, configKey=%s)",
                app,
                env,
                namespace,
                configKey,
            )
            false
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
    ): DeleteAndIncrementVersionResult {
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
                        DeleteAndIncrementVersionResult.NotFound
                    } else {
                        val version = incrementVersion(connection, app, env)
                        connection.commit()
                        DeleteAndIncrementVersionResult.Deleted(version)
                    }
                } catch (error: SQLException) {
                    rollbackSafely(connection, error)
                    DeleteAndIncrementVersionResult.Failed(error)
                } finally {
                    connection.autoCommit = originalAutoCommit
                }
            }
        } catch (error: SQLException) {
            DeleteAndIncrementVersionResult.Failed(error)
        }
    }

    private fun mapDocument(resultSet: ResultSet): ConfigDocument {
        return ConfigDocument(
            id = resultSet.getLong("id"),
            app = resultSet.getString("app"),
            env = resultSet.getString("env"),
            namespace = resultSet.getString("namespace"),
            configKey = resultSet.getString("config_key"),
            contentJson = resultSet.getString("content"),
            createdAt = resultSet.getTimestamp("created_at").toInstant(),
            updatedAt = resultSet.getTimestamp("updated_at").toInstant(),
            updatedBy = resultSet.getString("updated_by"),
        )
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
        private val LOG = Logger.getLogger(ConfigDocumentRepository::class.java)

        private const val SELECT_ALL =
            """
            SELECT id, app, env, namespace, config_key, content, created_at, updated_at, updated_by
            FROM config_documents
            WHERE app = ? AND env = ?
            ORDER BY namespace, config_key
            """

        private const val SELECT_BY_NAMESPACE =
            """
            SELECT id, app, env, namespace, config_key, content, created_at, updated_at, updated_by
            FROM config_documents
            WHERE app = ? AND env = ? AND namespace = ?
            ORDER BY config_key
            """

        private const val SELECT_ONE =
            """
            SELECT id, app, env, namespace, config_key, content, created_at, updated_at, updated_by
            FROM config_documents
            WHERE app = ? AND env = ? AND namespace = ? AND config_key = ?
            """

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
