package gg.grounds.api

import gg.grounds.domain.ConfigDocument
import gg.grounds.events.ConfigChangePublisher
import gg.grounds.grpc.config.DeleteDocumentRequest
import gg.grounds.grpc.config.DeleteDocumentResponse
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.ListDocumentsRequest
import gg.grounds.grpc.config.ListDocumentsResponse
import gg.grounds.grpc.config.PutDocumentRequest
import gg.grounds.grpc.config.PutDocumentResponse
import gg.grounds.persistence.ConfigDocumentRepository
import io.grpc.Status
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class ConfigAdminDocumentService
@Inject
constructor(
    private val documentRepository: ConfigDocumentRepository,
    private val changePublisher: ConfigChangePublisher,
) {
    fun listDocuments(request: ListDocumentsRequest): ListDocumentsResponse {
        val context =
            ConfigRequestContexts.toNamespaceContext(request.app, request.env, request.namespace)
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

    fun putDocument(request: PutDocumentRequest): PutDocumentResponse {
        val context =
            ConfigRequestContexts.toDocumentContext(
                request.app,
                request.env,
                request.namespace,
                request.configKey,
            )
        val updatedBy = request.updatedBy.trim().ifEmpty { null }
        val document =
            ConfigDocument(
                app = context.app,
                env = context.env,
                namespace = context.namespace,
                configKey = context.configKey,
                contentJson = request.contentJson,
                updatedBy = updatedBy,
            )
        val version =
            when (val result = documentRepository.upsertAndIncrementVersion(document)) {
                is ConfigDocumentRepository.UpsertAndIncrementVersionResult.Updated ->
                    result.version
                is ConfigDocumentRepository.UpsertAndIncrementVersionResult.Failed -> {
                    LOG.errorf(
                        result.cause,
                        "Failed to put config document (app=%s, env=%s, namespace=%s, configKey=%s)",
                        context.app,
                        context.env,
                        context.namespace,
                        context.configKey,
                    )
                    throw Status.INTERNAL.withDescription(
                            "Failed to put config document (app=${context.app}, env=${context.env}, namespace=${context.namespace}, configKey=${context.configKey})"
                        )
                        .asRuntimeException()
                }
            }
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

    fun deleteDocument(request: DeleteDocumentRequest): DeleteDocumentResponse {
        val context =
            ConfigRequestContexts.toDocumentContext(
                request.app,
                request.env,
                request.namespace,
                request.configKey,
            )
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

    companion object {
        private val LOG = Logger.getLogger(ConfigAdminDocumentService::class.java)
    }
}
