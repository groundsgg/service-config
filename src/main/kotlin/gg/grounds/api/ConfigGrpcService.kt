package gg.grounds.api

import gg.grounds.grpc.config.ConfigService
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.grpc.config.GetNamespaceSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotIfNewerRequest
import gg.grounds.grpc.config.GetSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotResponse
import gg.grounds.grpc.config.SyncDefaultsRequest
import gg.grounds.grpc.config.SyncDefaultsResponse
import gg.grounds.persistence.ConfigDocumentRepository
import gg.grounds.persistence.ConfigVersionRepository
import io.quarkus.grpc.GrpcService
import io.smallrye.common.annotation.Blocking
import io.smallrye.mutiny.Uni
import jakarta.inject.Inject
import org.jboss.logging.Logger

@GrpcService
@Blocking
class ConfigGrpcService
@Inject
constructor(
    private val documentRepository: ConfigDocumentRepository,
    private val versionRepository: ConfigVersionRepository,
) : ConfigService {
    override fun getSnapshot(request: GetSnapshotRequest): Uni<GetSnapshotResponse> {
        return Uni.createFrom().item { handleGetSnapshot(request) }
    }

    override fun getSnapshotIfNewer(request: GetSnapshotIfNewerRequest): Uni<GetSnapshotResponse> {
        return Uni.createFrom().item { handleGetSnapshotIfNewer(request) }
    }

    override fun getNamespaceSnapshot(
        request: GetNamespaceSnapshotRequest
    ): Uni<GetSnapshotResponse> {
        return Uni.createFrom().item { handleGetNamespaceSnapshot(request) }
    }

    override fun getDocument(request: GetDocumentRequest): Uni<GetDocumentResponse> {
        return Uni.createFrom().item { handleGetDocument(request) }
    }

    override fun syncDefaults(request: SyncDefaultsRequest): Uni<SyncDefaultsResponse> {
        return Uni.createFrom().item { handleSyncDefaults(request) }
    }

    private fun handleGetSnapshot(request: GetSnapshotRequest): GetSnapshotResponse {
        val app = request.app.trim()
        val env = request.env.trim()
        val version = versionRepository.getVersion(app, env)
        val documents = documentRepository.findAll(app, env)
        return GetSnapshotResponse.newBuilder()
            .setChanged(true)
            .setVersion(version)
            .addAllDocuments(ConfigProtoMapper.toProtoList(documents))
            .build()
    }

    private fun handleGetSnapshotIfNewer(request: GetSnapshotIfNewerRequest): GetSnapshotResponse {
        val app = request.app.trim()
        val env = request.env.trim()
        val knownVersion = request.knownVersion
        val currentVersion = versionRepository.getVersion(app, env)
        if (currentVersion <= knownVersion) {
            return GetSnapshotResponse.newBuilder()
                .setChanged(false)
                .setVersion(currentVersion)
                .build()
        }
        val documents = documentRepository.findAll(app, env)
        return GetSnapshotResponse.newBuilder()
            .setChanged(true)
            .setVersion(currentVersion)
            .addAllDocuments(ConfigProtoMapper.toProtoList(documents))
            .build()
    }

    private fun handleGetNamespaceSnapshot(
        request: GetNamespaceSnapshotRequest
    ): GetSnapshotResponse {
        val app = request.app.trim()
        val env = request.env.trim()
        val namespace = request.namespace.trim()
        val version = versionRepository.getVersion(app, env)
        val documents = documentRepository.findByNamespace(app, env, namespace)
        return GetSnapshotResponse.newBuilder()
            .setChanged(true)
            .setVersion(version)
            .addAllDocuments(ConfigProtoMapper.toProtoList(documents))
            .build()
    }

    private fun handleGetDocument(request: GetDocumentRequest): GetDocumentResponse {
        val app = request.app.trim()
        val env = request.env.trim()
        val namespace = request.namespace.trim()
        val configKey = request.configKey.trim()
        val document = documentRepository.findOne(app, env, namespace, configKey)
        val builder = GetDocumentResponse.newBuilder()
        if (document != null) {
            builder.setDocument(ConfigProtoMapper.toProto(document))
        }
        return builder.build()
    }

    private fun handleSyncDefaults(request: SyncDefaultsRequest): SyncDefaultsResponse {
        val app = request.app.trim()
        val env = request.env.trim()
        val createdKeys = mutableListOf<String>()
        for (configDefault in request.defaultsList) {
            val namespace = configDefault.namespace.trim()
            val configKey = configDefault.configKey.trim()
            val defaultContent = configDefault.defaultContentJson
            val created =
                documentRepository.insertIfNotExists(app, env, namespace, configKey, defaultContent)
            if (created) {
                createdKeys.add("$namespace/$configKey")
                LOG.infof(
                    "Created default config (app=%s, env=%s, namespace=%s, configKey=%s)",
                    app,
                    env,
                    namespace,
                    configKey,
                )
            }
        }
        val version =
            if (createdKeys.isNotEmpty()) {
                versionRepository.incrementVersion(app, env)
            } else {
                versionRepository.getVersion(app, env)
            }
        return SyncDefaultsResponse.newBuilder()
            .setVersion(version)
            .addAllCreatedKeys(createdKeys)
            .build()
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigGrpcService::class.java)
    }
}
