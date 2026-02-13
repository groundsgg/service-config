package gg.grounds.api

import gg.grounds.grpc.config.ConfigAdminService
import gg.grounds.grpc.config.DeleteDocumentRequest
import gg.grounds.grpc.config.DeleteDocumentResponse
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.grpc.config.ListDocumentsRequest
import gg.grounds.grpc.config.ListDocumentsResponse
import gg.grounds.grpc.config.PutDocumentRequest
import gg.grounds.grpc.config.PutDocumentResponse
import io.quarkus.grpc.GrpcService
import io.smallrye.common.annotation.Blocking
import io.smallrye.mutiny.Uni
import jakarta.inject.Inject

@GrpcService
@Blocking
class ConfigAdminGrpcService
@Inject
constructor(private val documentService: ConfigAdminDocumentService) : ConfigAdminService {
    override fun listDocuments(request: ListDocumentsRequest): Uni<ListDocumentsResponse> {
        return Uni.createFrom().item { documentService.listDocuments(request) }
    }

    override fun getDocument(request: GetDocumentRequest): Uni<GetDocumentResponse> {
        return Uni.createFrom().item { documentService.getDocument(request) }
    }

    override fun putDocument(request: PutDocumentRequest): Uni<PutDocumentResponse> {
        return Uni.createFrom().item { documentService.putDocument(request) }
    }

    override fun deleteDocument(request: DeleteDocumentRequest): Uni<DeleteDocumentResponse> {
        return Uni.createFrom().item { documentService.deleteDocument(request) }
    }
}
