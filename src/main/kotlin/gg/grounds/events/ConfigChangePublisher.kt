package gg.grounds.events

import com.fasterxml.jackson.databind.ObjectMapper
import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.Nats
import io.nats.client.Options
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Duration
import java.time.Instant
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigChangePublisher
@Inject
constructor(
    @param:ConfigProperty(name = "nats.url") private val natsUrl: String,
    @param:ConfigProperty(name = "nats.max-reconnects")
    private val maxReconnects: Int = DEFAULT_MAX_RECONNECTS,
    @param:ConfigProperty(name = "nats.reconnect-wait-seconds")
    private val reconnectWaitSeconds: Long = DEFAULT_RECONNECT_WAIT_SECONDS,
    private val objectMapper: ObjectMapper,
) {
    constructor(
        natsUrl: String,
        objectMapper: ObjectMapper,
    ) : this(
        natsUrl = natsUrl,
        maxReconnects = DEFAULT_MAX_RECONNECTS,
        reconnectWaitSeconds = DEFAULT_RECONNECT_WAIT_SECONDS,
        objectMapper = objectMapper,
    )

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
            val options =
                Options.Builder()
                    .server(natsUrl)
                    .maxReconnects(maxReconnects)
                    .reconnectWait(Duration.ofSeconds(reconnectWaitSeconds))
                    .connectionListener(
                        ConnectionListener { connected, event ->
                            onConnectionEvent(connected, event)
                        }
                    )
                    .build()
            connection = Nats.connect(options)
            LOG.infof(
                "Connected to NATS successfully (url=%s, maxReconnects=%d, reconnectWaitSeconds=%d)",
                natsUrl,
                maxReconnects,
                reconnectWaitSeconds,
            )
        } catch (error: Exception) {
            LOG.errorf(
                error,
                "Failed to connect to NATS (url=%s, maxReconnects=%d, reconnectWaitSeconds=%d)",
                natsUrl,
                maxReconnects,
                reconnectWaitSeconds,
            )
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
        val conn = connection
        if (conn == null || conn.status != Connection.Status.CONNECTED) {
            LOG.warnf(
                "Skipped config change publish (reason=nats_not_connected, app=%s, env=%s, version=%d, natsStatus=%s)",
                app,
                env,
                version,
                connectionStatus(),
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

    fun isConnected(): Boolean = connection?.status == Connection.Status.CONNECTED

    fun connectionStatus(): String = connection?.status?.name ?: "NOT_CONNECTED"

    fun close() {
        try {
            connection?.close()
            connection = null
            LOG.info("Disconnected from NATS successfully")
        } catch (error: Exception) {
            LOG.errorf(error, "Failed to close NATS connection (url=%s)", natsUrl)
        }
    }

    internal fun buildPayload(
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
                configKey?.let { put("configKey", it) }
                put("timestamp", Instant.now().toString())
            }
        return payload.toString()
    }

    private fun onConnectionEvent(connection: Connection, event: ConnectionListener.Events) {
        when (event) {
            ConnectionListener.Events.CONNECTED ->
                LOG.infof(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
            ConnectionListener.Events.RECONNECTED ->
                LOG.infof(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
            ConnectionListener.Events.DISCONNECTED ->
                LOG.warnf(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
            ConnectionListener.Events.RESUBSCRIBED ->
                LOG.infof(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
            ConnectionListener.Events.DISCOVERED_SERVERS ->
                LOG.infof(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
            ConnectionListener.Events.LAME_DUCK ->
                LOG.warnf(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
            ConnectionListener.Events.CLOSED ->
                LOG.warnf(
                    "NATS connection event received (event=%s, status=%s)",
                    event,
                    connection.status,
                )
        }
    }

    companion object {
        private const val DEFAULT_MAX_RECONNECTS = -1
        private const val DEFAULT_RECONNECT_WAIT_SECONDS = 2L
        private val LOG = Logger.getLogger(ConfigChangePublisher::class.java)
    }
}
