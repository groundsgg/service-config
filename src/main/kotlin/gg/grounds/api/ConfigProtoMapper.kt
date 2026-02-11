package gg.grounds.api

import gg.grounds.domain.ConfigDocument as DomainConfigDocument
import gg.grounds.grpc.config.ConfigDocument as ProtoConfigDocument

object ConfigProtoMapper {
    fun toProto(document: DomainConfigDocument): ProtoConfigDocument {
        return ProtoConfigDocument.newBuilder()
            .setNamespace(document.namespace)
            .setConfigKey(document.configKey)
            .setContentJson(document.contentJson)
            .build()
    }

    fun toProtoList(documents: List<DomainConfigDocument>): List<ProtoConfigDocument> {
        return documents.map { toProto(it) }
    }
}
