package gg.grounds.api

import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.persistence.ConfigDocumentRepository
import io.grpc.Status

object ConfigDocumentLookup {
    fun getDocumentResponse(
        documentRepository: ConfigDocumentRepository,
        context: ConfigRequestContexts.DocumentContext,
    ): GetDocumentResponse {
        val document =
            documentRepository.findOne(
                context.app,
                context.env,
                context.namespace,
                context.configKey,
            )
        if (document == null) {
            throw Status.NOT_FOUND.withDescription(
                    "Config document not found (app=${context.app}, env=${context.env}, namespace=${context.namespace}, configKey=${context.configKey})"
                )
                .asRuntimeException()
        }
        return GetDocumentResponse.newBuilder()
            .setDocument(ConfigProtoMapper.toProto(document))
            .build()
    }
}
