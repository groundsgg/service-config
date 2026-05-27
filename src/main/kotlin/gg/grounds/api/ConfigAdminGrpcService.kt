package gg.grounds.api

import gg.grounds.auth.AuthGuard
import gg.grounds.grpc.config.ConfigAdminService
import gg.grounds.grpc.config.CreateDocumentRequest
import gg.grounds.grpc.config.CreateDocumentResponse
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

/**
 * Admin facade for config documents. All methods are admin-gated — the consumer-facing read API
 * lives in [ConfigGrpcService], which scopes results to the caller's project. Methods here can
 * list/edit any document and so require the platform-admin or config-admin SA (see [AuthGuard]).
 */
@GrpcService
@Blocking
class ConfigAdminGrpcService
@Inject
constructor(private val documentService: ConfigAdminDocumentService) : ConfigAdminService {
    override fun listDocuments(request: ListDocumentsRequest): Uni<ListDocumentsResponse> {
        AuthGuard.requireAdmin("listDocuments")
        return Uni.createFrom().item { documentService.listDocuments(request) }
    }

    override fun getDocument(request: GetDocumentRequest): Uni<GetDocumentResponse> {
        AuthGuard.requireAdmin("getDocument")
        return Uni.createFrom().item { documentService.getDocument(request) }
    }

    override fun createDocument(request: CreateDocumentRequest): Uni<CreateDocumentResponse> {
        AuthGuard.requireAdmin("createDocument")
        return Uni.createFrom().item { documentService.createDocument(request) }
    }

    override fun putDocument(request: PutDocumentRequest): Uni<PutDocumentResponse> {
        AuthGuard.requireAdmin("putDocument")
        return Uni.createFrom().item { documentService.putDocument(request) }
    }

    override fun deleteDocument(request: DeleteDocumentRequest): Uni<DeleteDocumentResponse> {
        AuthGuard.requireAdmin("deleteDocument")
        return Uni.createFrom().item { documentService.deleteDocument(request) }
    }
}
