package gg.grounds.api

import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.persistence.ConfigDocumentRepository

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
        val builder = GetDocumentResponse.newBuilder()
        if (document != null) {
            builder.setDocument(ConfigProtoMapper.toProto(document))
        }
        return builder.build()
    }
}
