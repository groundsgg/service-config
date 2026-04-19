package gg.grounds.api

import gg.grounds.grpc.config.ConfigService
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetDocumentResponse
import gg.grounds.grpc.config.GetNamespaceSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotIfNewerRequest
import gg.grounds.grpc.config.GetSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotResponse
import gg.grounds.grpc.config.SyncDefaultsRequest
import gg.grounds.grpc.config.SyncDefaultsResponse
import io.quarkus.grpc.GrpcService
import io.smallrye.common.annotation.Blocking
import io.smallrye.mutiny.Uni
import jakarta.inject.Inject

@GrpcService
@Blocking
class ConfigGrpcService @Inject constructor(private val documentService: ConfigDocumentApiService) :
    ConfigService {
    override fun getSnapshot(request: GetSnapshotRequest): Uni<GetSnapshotResponse> {
        return Uni.createFrom().item { documentService.getSnapshot(request) }
    }

    override fun getSnapshotIfNewer(request: GetSnapshotIfNewerRequest): Uni<GetSnapshotResponse> {
        return Uni.createFrom().item { documentService.getSnapshotIfNewer(request) }
    }

    override fun getNamespaceSnapshot(
        request: GetNamespaceSnapshotRequest
    ): Uni<GetSnapshotResponse> {
        return Uni.createFrom().item { documentService.getNamespaceSnapshot(request) }
    }

    override fun getDocument(request: GetDocumentRequest): Uni<GetDocumentResponse> {
        return Uni.createFrom().item { documentService.getDocument(request) }
    }

    override fun syncDefaults(request: SyncDefaultsRequest): Uni<SyncDefaultsResponse> {
        return Uni.createFrom().item { documentService.syncDefaults(request) }
    }
}
