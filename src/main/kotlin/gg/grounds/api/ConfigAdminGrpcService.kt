package gg.grounds.api

import gg.grounds.auth.AuthGuard
import gg.grounds.auth.ConfigWritePolicy
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
 * Admin facade for config documents. The consumer-facing read API lives in [ConfigGrpcService],
 * which any authenticated caller may use; methods here can see and edit any document.
 *
 * Browsing is admin-only — listing and reading across every app is an operator's job, and the
 * platform-admin or config-admin SA is what grants it (see [AuthGuard]).
 *
 * Replacing and deleting one document additionally accept a writer named for that app in
 * [ConfigWritePolicy]. That is what lets a service own its own configuration — the proxies write
 * the network MOTD — without being handed every other app's along with it. Creating stays
 * admin-only: which documents exist in an app is a shape decision, and the create-or-replace path a
 * self-configuring service needs is [putDocument].
 */
@GrpcService
@Blocking
class ConfigAdminGrpcService
@Inject
constructor(
    private val documentService: ConfigAdminDocumentService,
    private val writePolicy: ConfigWritePolicy,
) : ConfigAdminService {
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
        writePolicy.requireWrite(request.app, "putDocument")
        return Uni.createFrom().item { documentService.putDocument(request) }
    }

    override fun deleteDocument(request: DeleteDocumentRequest): Uni<DeleteDocumentResponse> {
        writePolicy.requireWrite(request.app, "deleteDocument")
        return Uni.createFrom().item { documentService.deleteDocument(request) }
    }
}
