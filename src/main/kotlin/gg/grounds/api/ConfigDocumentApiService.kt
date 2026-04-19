package gg.grounds.api

import com.fasterxml.jackson.databind.ObjectMapper
import gg.grounds.domain.ConfigDocument
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.ConfigDocumentKey
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetNamespaceSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotIfNewerRequest
import gg.grounds.grpc.config.GetSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotResponse
import gg.grounds.grpc.config.SyncDefaultsRequest
import gg.grounds.grpc.config.SyncDefaultsResponse
import gg.grounds.persistence.ConfigDocumentRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigDocumentApiService
@Inject
constructor(
    private val documentRepository: ConfigDocumentRepository,
    private val changePublisher: ConfigChangePublisher,
    private val objectMapper: ObjectMapper,
) {
    fun getSnapshot(request: GetSnapshotRequest): GetSnapshotResponse {
        val context = ConfigRequestContexts.toAppEnvContext(request.app, request.env)
        val snapshot = documentRepository.getSnapshot(context.app, context.env)
        return toChangedSnapshotResponse(snapshot.version, snapshot.documents)
    }

    fun getSnapshotIfNewer(request: GetSnapshotIfNewerRequest): GetSnapshotResponse {
        val context = ConfigRequestContexts.toAppEnvContext(request.app, request.env)
        val snapshot =
            documentRepository.getSnapshotIfNewer(context.app, context.env, request.knownVersion)
        if (snapshot == null) {
            val currentVersion = documentRepository.getVersion(context.app, context.env)
            return GetSnapshotResponse.newBuilder()
                .setChanged(false)
                .setVersion(currentVersion)
                .build()
        }
        return toChangedSnapshotResponse(snapshot.version, snapshot.documents)
    }

    fun getNamespaceSnapshot(request: GetNamespaceSnapshotRequest): GetSnapshotResponse {
        val context =
            ConfigRequestContexts.toNamespaceContext(request.app, request.env, request.namespace)
        val snapshot =
            documentRepository.getNamespaceSnapshot(context.app, context.env, context.namespace)
        return toChangedSnapshotResponse(snapshot.version, snapshot.documents)
    }

    fun getDocument(request: GetDocumentRequest) =
        ConfigDocumentLookup.getDocumentResponse(
            documentRepository,
            ConfigRequestContexts.toDocumentContext(
                request.app,
                request.env,
                request.namespace,
                request.configKey,
            ),
        )

    fun syncDefaults(request: SyncDefaultsRequest): SyncDefaultsResponse {
        val context = ConfigRequestContexts.toAppEnvContext(request.app, request.env)
        val defaults =
            request.defaultsList.map { configDefault ->
                validateJsonContent(configDefault.defaultContentJson)
                ConfigDocumentRepository.DefaultConfig(
                    namespace =
                        ConfigRequestContexts.requireSegment("namespace", configDefault.namespace),
                    configKey =
                        ConfigRequestContexts.requireSegment("configKey", configDefault.configKey),
                    defaultContentJson = configDefault.defaultContentJson,
                )
            }
        val syncResult = documentRepository.syncDefaults(context.app, context.env, defaults)
        val createdKeys =
            syncResult.createdDefaults.map { createdDefault ->
                ConfigDocumentKey.newBuilder()
                    .setNamespace(createdDefault.namespace)
                    .setConfigKey(createdDefault.configKey)
                    .build()
            }
        for (createdDefault in syncResult.createdDefaults) {
            LOG.infof(
                "Created default config (app=%s, env=%s, namespace=%s, configKey=%s)",
                context.app,
                context.env,
                createdDefault.namespace,
                createdDefault.configKey,
            )
        }
        if (createdKeys.isNotEmpty()) {
            val changePublishResult =
                changePublisher.publishChange(context.app, context.env, syncResult.version)
            LOG.infof(
                "Default config sync completed successfully (app=%s, env=%s, createdCount=%d, version=%d, changePublishResult=%s, changeDelivery=best_effort)",
                context.app,
                context.env,
                createdKeys.size,
                syncResult.version,
                changePublishResult.name.lowercase(),
            )
        }
        return SyncDefaultsResponse.newBuilder()
            .setVersion(syncResult.version)
            .addAllCreatedKeys(createdKeys)
            .build()
    }

    private fun validateJsonContent(contentJson: String) {
        try {
            objectMapper.readTree(contentJson)
        } catch (_: Exception) {
            throw io.grpc.Status.INVALID_ARGUMENT.withDescription(
                    "defaultContentJson must be valid JSON"
                )
                .asRuntimeException()
        }
    }

    private fun toChangedSnapshotResponse(
        version: Long,
        documents: List<ConfigDocument>,
    ): GetSnapshotResponse {
        return GetSnapshotResponse.newBuilder()
            .setChanged(true)
            .setVersion(version)
            .addAllDocuments(ConfigProtoMapper.toProtoList(documents))
            .build()
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigDocumentApiService::class.java)
    }
}
