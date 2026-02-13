package gg.grounds.persistence

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.SQLException
import javax.sql.DataSource
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigVersionRepository @Inject constructor(private val dataSource: DataSource) {
    fun getVersion(app: String, env: String): Long {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(SELECT_VERSION).use { statement ->
                    statement.setString(1, app)
                    statement.setString(2, env)
                    statement.executeQuery().use { resultSet ->
                        if (resultSet.next()) resultSet.getLong("version") else 0L
                    }
                }
            }
        } catch (error: SQLException) {
            LOG.errorf(error, "Failed to get config version (app=%s, env=%s)", app, env)
            throw error
        }
    }

    fun incrementVersion(app: String, env: String): Long {
        return try {
            dataSource.connection.use { connection ->
                connection.prepareStatement(INCREMENT_VERSION).use { statement ->
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
        } catch (error: SQLException) {
            LOG.errorf(error, "Failed to increment config version (app=%s, env=%s)", app, env)
            throw error
        }
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigVersionRepository::class.java)

        private const val SELECT_VERSION =
            """
            SELECT version
            FROM config_app_versions
            WHERE app = ? AND env = ?
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
