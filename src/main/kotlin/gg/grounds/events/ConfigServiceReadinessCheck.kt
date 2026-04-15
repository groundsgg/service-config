package gg.grounds.events

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import javax.sql.DataSource
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Readiness

@Readiness
@ApplicationScoped
class ConfigServiceReadinessCheck
@Inject
constructor(
    private val dataSource: DataSource,
    private val changePublisher: ConfigChangePublisher,
) : HealthCheck {
    override fun call(): HealthCheckResponse {
        val databaseUp = isDatabaseReachable()
        val natsUp = changePublisher.isConnected()
        val response =
            HealthCheckResponse.named("config-service-readiness")
                .withData("database", if (databaseUp) "up" else "down")
                .withData("nats", if (natsUp) "up" else "down")
                .withData("natsStatus", changePublisher.connectionStatus())
        return if (databaseUp) {
            response.up().build()
        } else {
            response.down().build()
        }
    }

    private fun isDatabaseReachable(): Boolean {
        return try {
            dataSource.connection.use { connection -> connection.isValid(1) }
        } catch (_: Exception) {
            false
        }
    }
}
