package gg.grounds.api

import gg.grounds.domain.ConfigDocument
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.ConfigAdminService
import gg.grounds.grpc.config.DeleteDocumentRequest
import gg.grounds.grpc.config.DeleteDocumentResponse
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.grpc.config.ListDocumentsRequest
import gg.grounds.grpc.config.ListDocumentsResponse
import gg.grounds.grpc.config.PutDocumentRequest
import gg.grounds.grpc.config.PutDocumentResponse
import gg.grounds.persistence.ConfigDocumentRepository
import gg.grounds.persistence.ConfigVersionRepository
import io.quarkus.grpc.GrpcService
import io.smallrye.common.annotation.Blocking
import io.smallrye.mutiny.Uni
import jakarta.inject.Inject
import org.jboss.logging.Logger

@GrpcService
@Blocking
class ConfigAdminGrpcService
@Inject
constructor(
    private val documentRepository: ConfigDocumentRepository,
    private val versionRepository: ConfigVersionRepository,
    private val changePublisher: ConfigChangePublisher,
) : ConfigAdminService {
    override fun listDocuments(request: ListDocumentsRequest): Uni<ListDocumentsResponse> {
        return Uni.createFrom().item { handleListDocuments(request) }
    }

    override fun getDocument(request: GetDocumentRequest): Uni<GetDocumentResponse> {
        return Uni.createFrom().item { handleGetDocument(request) }
    }

    override fun putDocument(request: PutDocumentRequest): Uni<PutDocumentResponse> {
        return Uni.createFrom().item { handlePutDocument(request) }
    }

    override fun deleteDocument(request: DeleteDocumentRequest): Uni<DeleteDocumentResponse> {
        return Uni.createFrom().item { handleDeleteDocument(request) }
    }

    private fun handleListDocuments(request: ListDocumentsRequest): ListDocumentsResponse {
        val context = toNamespaceContext(request.app, request.env, request.namespace)
        val documents =
            if (context.namespace.isNotEmpty()) {
                documentRepository.findByNamespace(context.app, context.env, context.namespace)
            } else {
                documentRepository.findAll(context.app, context.env)
            }
        return ListDocumentsResponse.newBuilder()
            .addAllDocuments(ConfigProtoMapper.toProtoList(documents))
            .build()
    }

    private fun handleGetDocument(request: GetDocumentRequest): GetDocumentResponse {
        val context =
            toDocumentContext(request.app, request.env, request.namespace, request.configKey)
        val document =
            documentRepository.findOne(
                context.app,
                context.env,
                context.namespace,
                context.configKey,
            )
        val builder = GetDocumentResponse.newBuilder()
        if (document != null) {
            builder.setDocument(ConfigProtoMapper.toProto(document))
        }
        return builder.build()
    }

    private fun handlePutDocument(request: PutDocumentRequest): PutDocumentResponse {
        val context =
            toDocumentContext(request.app, request.env, request.namespace, request.configKey)
        val contentJson = request.contentJson
        val updatedBy = request.updatedBy.trim().ifEmpty { null }
        val document =
            ConfigDocument(
                app = context.app,
                env = context.env,
                namespace = context.namespace,
                configKey = context.configKey,
                contentJson = contentJson,
                updatedBy = updatedBy,
            )
        val success = documentRepository.upsert(document)
        if (!success) {
            LOG.errorf(
                "Failed to put config document (app=%s, env=%s, namespace=%s, configKey=%s)",
                context.app,
                context.env,
                context.namespace,
                context.configKey,
            )
            return PutDocumentResponse.newBuilder().setVersion(0).build()
        }
        val version = versionRepository.incrementVersion(context.app, context.env)
        changePublisher.publishChange(
            context.app,
            context.env,
            version,
            context.namespace,
            context.configKey,
        )
        LOG.infof(
            "Config document updated (app=%s, env=%s, namespace=%s, configKey=%s, version=%d, updatedBy=%s)",
            context.app,
            context.env,
            context.namespace,
            context.configKey,
            version,
            updatedBy,
        )
        return PutDocumentResponse.newBuilder().setVersion(version).build()
    }

    private fun handleDeleteDocument(request: DeleteDocumentRequest): DeleteDocumentResponse {
        val context =
            toDocumentContext(request.app, request.env, request.namespace, request.configKey)
        return when (
            val result =
                documentRepository.deleteAndIncrementVersion(
                    context.app,
                    context.env,
                    context.namespace,
                    context.configKey,
                )
        ) {
            is ConfigDocumentRepository.DeleteAndIncrementVersionResult.Deleted -> {
                changePublisher.publishChange(
                    context.app,
                    context.env,
                    result.version,
                    context.namespace,
                    context.configKey,
                )
                LOG.infof(
                    "Config document deleted successfully (app=%s, env=%s, namespace=%s, configKey=%s, version=%d)",
                    context.app,
                    context.env,
                    context.namespace,
                    context.configKey,
                    result.version,
                )
                DeleteDocumentResponse.newBuilder().setDeleted(true).build()
            }
            ConfigDocumentRepository.DeleteAndIncrementVersionResult.NotFound ->
                DeleteDocumentResponse.newBuilder().setDeleted(false).build()
            is ConfigDocumentRepository.DeleteAndIncrementVersionResult.Failed -> {
                LOG.errorf(
                    result.cause,
                    "Failed to delete config document (app=%s, env=%s, namespace=%s, configKey=%s)",
                    context.app,
                    context.env,
                    context.namespace,
                    context.configKey,
                )
                DeleteDocumentResponse.newBuilder().setDeleted(false).build()
            }
        }
    }

    private fun toNamespaceContext(app: String, env: String, namespace: String): NamespaceContext {
        return NamespaceContext(app.trim(), env.trim(), namespace.trim())
    }

    private fun toDocumentContext(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
    ): DocumentContext {
        val namespaceContext = toNamespaceContext(app, env, namespace)
        return DocumentContext(
            app = namespaceContext.app,
            env = namespaceContext.env,
            namespace = namespaceContext.namespace,
            configKey = configKey.trim(),
        )
    }

    private data class NamespaceContext(val app: String, val env: String, val namespace: String)

    private data class DocumentContext(
        val app: String,
        val env: String,
        val namespace: String,
        val configKey: String,
    )

    companion object {
        private val LOG = Logger.getLogger(ConfigAdminGrpcService::class.java)
    }
}
