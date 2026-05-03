package gg.grounds.events

import com.google.protobuf.Timestamp
import gg.grounds.events.config.ConfigChanged
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

/**
 * Publishes ConfigChanged events on NATS for cross-service-cache
 * invalidation. Other services subscribe to `config.<app>.<env>.changed`
 * (or wildcards thereof) and re-fetch their snapshot via the gRPC
 * ConfigService when they see this — the event itself only carries
 * the version, not the new payload.
 *
 * Migrated 2026-05-03 from hand-rolled Jackson-JSON to the typed
 * proto schema in `library-grpc-contracts:events`. Wire format is
 * proto3 binary now (smaller + type-safe on the subscriber side).
 *
 * Subject pattern: `config.{app}.{env}.changed`
 *   examples:
 *     config.service-player.prod.changed
 *     config.plugin-social.dev-lusu.changed
 *
 * See: groundsgg/library-grpc-contracts events/src/main/proto/config_events.proto
 */
@ApplicationScoped
class ConfigChangePublisher
@Inject
constructor(@param:ConfigProperty(name = "nats.url") private val natsUrl: String) {
    private var connection: Connection? = null

    @Synchronized
    fun connect() {
        val existingConnection = connection
        if (existingConnection != null) {
            if (existingConnection.status == Connection.Status.CONNECTED) {
                return
            }
            try {
                existingConnection.close()
            } catch (error: Exception) {
                LOG.warnf(error, "Failed to close stale NATS connection (url=%s)", natsUrl)
            } finally {
                connection = null
            }
        }
        try {
            val options = Options.Builder().server(natsUrl).build()
            connection = Nats.connect(options)
            LOG.infof("Connected to NATS (url=%s)", natsUrl)
        } catch (error: Exception) {
            LOG.errorf(error, "Failed to connect to NATS (url=%s)", natsUrl)
            connection = null
        }
    }

    fun publishChange(
        app: String,
        env: String,
        version: Long,
        namespace: String? = null,
        configKey: String? = null,
    ) {
        val conn = connectedConnection()
        if (conn == null || conn.status != Connection.Status.CONNECTED) {
            LOG.warnf(
                "Skipped config change publish (reason=nats_not_connected, app=%s, env=%s, version=%d)",
                app,
                env,
                version,
            )
            return
        }
        val subject = "config.$app.$env.changed"
        val payload = buildPayload(app, env, version, namespace, configKey)
        try {
            conn.publish(subject, payload)
            LOG.debugf("Published config change (subject=%s, version=%d)", subject, version)
        } catch (error: Exception) {
            LOG.errorf(error, "Failed to publish config change (subject=%s)", subject)
        }
    }

    private fun connectedConnection(): Connection? {
        val currentConnection = connection
        if (currentConnection != null && currentConnection.status == Connection.Status.CONNECTED) {
            return currentConnection
        }
        connect()
        return connection
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

    /**
     * Build a wire-format byte payload from the typed proto
     * ConfigChanged message. `namespace` and `config_key` are optional
     * (proto3 `optional` keyword) — not setting them leaves the field
     * absent on the wire.
     */
    private fun buildPayload(
        app: String,
        env: String,
        version: Long,
        namespace: String?,
        configKey: String?,
    ): ByteArray {
        val now = Instant.now()
        val builder =
            ConfigChanged.newBuilder()
                .setApp(app)
                .setEnv(env)
                .setVersion(version)
                .setTimestamp(
                    Timestamp.newBuilder()
                        .setSeconds(now.epochSecond)
                        .setNanos(now.nano)
                        .build()
                )
        namespace?.let { builder.namespace = it }
        configKey?.let { builder.configKey = it }
        return builder.build().toByteArray()
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigChangePublisher::class.java)
    }
}
