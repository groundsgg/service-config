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
internal class ConfigDocumentReadRepository
@Inject
constructor(private val dataSource: DataSource) {
    fun findAll(app: String, env: String): List<ConfigDocument> {
        return try {
            dataSource.connection.use { connection -> findAll(connection, app, env) }
        } catch (error: SQLException) {
            LOG.errorf(
                "Failed to list config documents (app=%s, env=%s, reason=%s)",
                app,
                env,
                errorReason(error),
            )
            throw error
        }
    }

    internal fun findAll(connection: Connection, app: String, env: String): List<ConfigDocument> {
        return connection.prepareStatement(SELECT_ALL).use { statement ->
            statement.setString(1, app)
            statement.setString(2, env)
            statement.executeQuery().use { resultSet -> readDocuments(resultSet) }
        }
    }

    fun findByNamespace(app: String, env: String, namespace: String): List<ConfigDocument> {
        return try {
            dataSource.connection.use { connection ->
                findByNamespace(connection, app, env, namespace)
            }
        } catch (error: SQLException) {
            LOG.errorf(
                "Failed to list config documents (app=%s, env=%s, namespace=%s, reason=%s)",
                app,
                env,
                namespace,
                errorReason(error),
            )
            throw error
        }
    }

    internal fun findByNamespace(
        connection: Connection,
        app: String,
        env: String,
        namespace: String,
    ): List<ConfigDocument> {
        return connection.prepareStatement(SELECT_BY_NAMESPACE).use { statement ->
            statement.setString(1, app)
            statement.setString(2, env)
            statement.setString(3, namespace)
            statement.executeQuery().use { resultSet -> readDocuments(resultSet) }
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
                "Failed to fetch config document (app=%s, env=%s, namespace=%s, configKey=%s, reason=%s)",
                app,
                env,
                namespace,
                configKey,
                errorReason(error),
            )
            throw error
        }
    }

    private fun readDocuments(resultSet: ResultSet): List<ConfigDocument> {
        val documents = mutableListOf<ConfigDocument>()
        while (resultSet.next()) {
            documents.add(mapDocument(resultSet))
        }
        return documents
    }

    private fun errorReason(error: SQLException): String {
        return error.message ?: error::class.java.simpleName
    }

    private fun mapDocument(resultSet: ResultSet): ConfigDocument {
        return ConfigDocument(
            id = resultSet.getLong("id"),
            app = resultSet.getString("app"),
            env = resultSet.getString("env"),
            namespace = resultSet.getString("namespace"),
            configKey = resultSet.getString("config_key"),
            contentJson = resultSet.getString("content"),
            version = resultSet.getLong("version"),
            createdAt = resultSet.getTimestamp("created_at").toInstant(),
            updatedAt = resultSet.getTimestamp("updated_at").toInstant(),
            updatedBy = resultSet.getString("updated_by"),
        )
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigDocumentReadRepository::class.java)

        private const val SELECT_ALL =
            """
            SELECT id, app, env, namespace, config_key, content, version, created_at, updated_at, updated_by
            FROM config_documents
            WHERE app = ? AND env = ?
            ORDER BY namespace, config_key
            """

        private const val SELECT_BY_NAMESPACE =
            """
            SELECT id, app, env, namespace, config_key, content, version, created_at, updated_at, updated_by
            FROM config_documents
            WHERE app = ? AND env = ? AND namespace = ?
            ORDER BY config_key
            """

        private const val SELECT_ONE =
            """
            SELECT id, app, env, namespace, config_key, content, version, created_at, updated_at, updated_by
            FROM config_documents
            WHERE app = ? AND env = ? AND namespace = ? AND config_key = ?
            """
    }
}
