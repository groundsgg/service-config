package gg.grounds.api

import com.fasterxml.jackson.databind.ObjectMapper
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
    private val objectMapper: ObjectMapper,
) {
    fun listDocuments(request: ListDocumentsRequest): ListDocumentsResponse {
        val context =
            ConfigRequestContexts.toNamespaceContext(
                request.app,
                request.env,
                request.namespace,
                allowEmptyNamespace = true,
            )
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
        validateJsonContent(request.contentJson)
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
        LOG.warnf(
            "Config document delete rejected (app=%s, env=%s, namespace=%s, configKey=%s, reason=delete_not_supported)",
            context.app,
            context.env,
            context.namespace,
            context.configKey,
        )
        throw Status.FAILED_PRECONDITION.withDescription(
                "DeleteDocument is not supported because config documents are the persisted runtime truth; use PutDocument to replace the value"
            )
            .asRuntimeException()
    }

    private fun validateJsonContent(contentJson: String) {
        try {
            objectMapper.readTree(contentJson)
        } catch (_: Exception) {
            throw Status.INVALID_ARGUMENT.withDescription("contentJson must be valid JSON")
                .asRuntimeException()
        }
    }

    companion object {
        private val LOG = Logger.getLogger(ConfigAdminDocumentService::class.java)
    }
}
