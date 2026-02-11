package gg.grounds.events

import com.fasterxml.jackson.databind.ObjectMapper
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigChangePublisher
@Inject
constructor(
    @param:ConfigProperty(name = "nats.url") private val natsUrl: String,
    private val objectMapper: ObjectMapper,
) {
    private var connection: Connection? = null

    fun connect() {
        if (connection != null) {
            return
        }
        try {
            val options = Options.Builder().server(natsUrl).build()
            connection = Nats.connect(options)
            LOG.infof("Connected to NATS (url=%s)", natsUrl)
        } catch (error: Exception) {
            LOG.errorf(error, "Failed to connect to NATS (url=%s)", natsUrl)
        }
    }

    fun publishChange(
        app: String,
        env: String,
        version: Long,
        namespace: String? = null,
        configKey: String? = null,
    ) {
        val conn = connection
        if (conn == null || conn.status != Connection.Status.CONNECTED) {
            LOG.warnf(
                "NATS not connected, skipping publish (app=%s, env=%s, version=%d)",
                app,
                env,
                version,
            )
            return
        }
        val subject = "config.$app.$env.changed"
        val payload = buildPayload(app, env, version, namespace, configKey)
        try {
            conn.publish(subject, payload.toByteArray(Charsets.UTF_8))
            LOG.debugf("Published config change (subject=%s, version=%d)", subject, version)
        } catch (error: Exception) {
            LOG.errorf(error, "Failed to publish config change (subject=%s)", subject)
        }
    }

    fun close() {
        try {
            connection?.close()
            connection = null
            LOG.info("Disconnected from NATS")
        } catch (error: Exception) {
            LOG.errorf(error, "Failed to close NATS connection")
        }
    }

    private fun buildPayload(
        app: String,
        env: String,
        version: Long,
        namespace: String?,
        configKey: String?,
    ): String {
        val payload =
            objectMapper.createObjectNode().apply {
                put("app", app)
                put("env", env)
                put("version", version)
                namespace?.let { put("namespace", it) }
                configKey?.let { put("config_key", it) }
                put("timestamp", Instant.now().toString())
            }
        return payload.toString()
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigChangePublisher::class.java)
    }
}
