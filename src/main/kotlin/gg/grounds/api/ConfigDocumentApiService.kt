package gg.grounds.api

import gg.grounds.domain.ConfigDocument
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetNamespaceSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotIfNewerRequest
import gg.grounds.grpc.config.GetSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotResponse
import gg.grounds.grpc.config.SyncDefaultsRequest
import gg.grounds.grpc.config.SyncDefaultsResponse
import gg.grounds.persistence.ConfigDocumentRepository
import gg.grounds.persistence.ConfigVersionRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigDocumentApiService
@Inject
constructor(
    private val documentRepository: ConfigDocumentRepository,
    private val versionRepository: ConfigVersionRepository,
    private val changePublisher: ConfigChangePublisher,
) {
    fun getSnapshot(request: GetSnapshotRequest): GetSnapshotResponse {
        val context = ConfigRequestContexts.toAppEnvContext(request.app, request.env)
        val version = versionRepository.getVersion(context.app, context.env)
        val documents = documentRepository.findAll(context.app, context.env)
        return toChangedSnapshotResponse(version, documents)
    }

    fun getSnapshotIfNewer(request: GetSnapshotIfNewerRequest): GetSnapshotResponse {
        val context = ConfigRequestContexts.toAppEnvContext(request.app, request.env)
        val knownVersion = request.knownVersion
        val currentVersion = versionRepository.getVersion(context.app, context.env)
        if (currentVersion <= knownVersion) {
            return GetSnapshotResponse.newBuilder()
                .setChanged(false)
                .setVersion(currentVersion)
                .build()
        }
        val documents = documentRepository.findAll(context.app, context.env)
        return toChangedSnapshotResponse(currentVersion, documents)
    }

    fun getNamespaceSnapshot(request: GetNamespaceSnapshotRequest): GetSnapshotResponse {
        val context =
            ConfigRequestContexts.toNamespaceContext(request.app, request.env, request.namespace)
        val version = versionRepository.getVersion(context.app, context.env)
        val documents =
            documentRepository.findByNamespace(context.app, context.env, context.namespace)
        return toChangedSnapshotResponse(version, documents)
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
                ConfigDocumentRepository.DefaultConfig(
                    namespace = configDefault.namespace.trim(),
                    configKey = configDefault.configKey.trim(),
                    defaultContentJson = configDefault.defaultContentJson,
                )
            }
        val createdDefaults =
            documentRepository.insertIfNotExists(context.app, context.env, defaults)
        val createdKeys = createdDefaults.map { "${it.namespace}/${it.configKey}" }
        for (createdDefault in createdDefaults) {
            LOG.infof(
                "Created default config (app=%s, env=%s, namespace=%s, configKey=%s)",
                context.app,
                context.env,
                createdDefault.namespace,
                createdDefault.configKey,
            )
        }
        val version =
            if (createdKeys.isNotEmpty()) {
                versionRepository.incrementVersion(context.app, context.env)
            } else {
                versionRepository.getVersion(context.app, context.env)
            }
        if (createdKeys.isNotEmpty()) {
            changePublisher.publishChange(context.app, context.env, version)
        }
        return SyncDefaultsResponse.newBuilder()
            .setVersion(version)
            .addAllCreatedKeys(createdKeys)
            .build()
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
