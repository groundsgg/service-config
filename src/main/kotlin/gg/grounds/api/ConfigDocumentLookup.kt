package gg.grounds.api

import gg.grounds.domain.ConfigErrorCode
import gg.grounds.domain.ConfigException
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
        if (document == null) {
            throw ConfigException(
                ConfigErrorCode.NOT_FOUND,
                "Config document not found (app=${context.app}, env=${context.env}, namespace=${context.namespace}, configKey=${context.configKey})",
            )
        }
        return GetDocumentResponse.newBuilder()
            .setDocument(ConfigProtoMapper.toProto(document))
            .build()
    }
}
