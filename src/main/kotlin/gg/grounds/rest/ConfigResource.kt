package gg.grounds.rest

import gg.grounds.api.ConfigDocumentApiService
import gg.grounds.grpc.config.ConfigDefault
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.GetNamespaceSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotIfNewerRequest
import gg.grounds.grpc.config.GetSnapshotRequest
import gg.grounds.grpc.config.GetSnapshotResponse
import gg.grounds.grpc.config.SyncDefaultsRequest
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.openapi.annotations.Operation
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse
import org.eclipse.microprofile.openapi.annotations.tags.Tag

/**
 * The read API every configured service uses.
 *
 * Open to any authenticated workload — reading an app's configuration is not privileged; changing
 * it is, and that lives in [ConfigAdminResource].
 *
 * The rules stay in [ConfigDocumentApiService], which the gRPC facade calls unchanged, so the two
 * transports cannot answer differently.
 */
@Path("/v1/config/apps/{app}/envs/{env}")
@Tag(name = "Config", description = "Versioned configuration documents.")
@Produces(MediaType.APPLICATION_JSON)
class ConfigResource @Inject constructor(private val service: ConfigDocumentApiService) {

    @GET
    @Path("/snapshot")
    @Operation(
        summary = "Read every document for an app and environment",
        description =
            "The response carries an `ETag` holding the snapshot's version. Send it back as " +
                "`If-None-Match` and an unchanged snapshot answers 304 with no body — which is " +
                "how a polling service avoids re-reading configuration that has not moved.",
    )
    @APIResponse(responseCode = "200", description = "The snapshot.")
    @APIResponse(responseCode = "304", description = "Nothing changed since the given version.")
    fun snapshot(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @HeaderParam("If-None-Match") ifNoneMatch: String?,
    ): Response {
        val knownVersion = ifNoneMatch?.let(::parseETag)
        val response =
            if (knownVersion == null) {
                service.getSnapshot(
                    GetSnapshotRequest.newBuilder()
                        .setApp(required(app, "app"))
                        .setEnv(required(env, "env"))
                        .build()
                )
            } else {
                service.getSnapshotIfNewer(
                    GetSnapshotIfNewerRequest.newBuilder()
                        .setApp(required(app, "app"))
                        .setEnv(required(env, "env"))
                        .setKnownVersion(knownVersion)
                        .build()
                )
            }

        if (!response.changed) {
            return Response.notModified(etag(response.version)).build()
        }
        return Response.ok(response.toSnapshot()).tag(etag(response.version)).build()
    }

    @GET
    @Path("/namespaces/{namespace}/snapshot")
    @Operation(summary = "Read one namespace of an app's documents")
    fun namespaceSnapshot(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @PathParam("namespace") namespace: String?,
    ): SnapshotResponse =
        service
            .getNamespaceSnapshot(
                GetNamespaceSnapshotRequest.newBuilder()
                    .setApp(required(app, "app"))
                    .setEnv(required(env, "env"))
                    .setNamespace(required(namespace, "namespace"))
                    .build()
            )
            .toSnapshot()

    @GET
    @Path("/namespaces/{namespace}/documents/{configKey}")
    @Operation(summary = "Read a single document")
    @APIResponse(responseCode = "404", description = "No such document.")
    fun document(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @PathParam("namespace") namespace: String?,
        @PathParam("configKey") configKey: String?,
    ): ConfigDocumentResponse =
        service
            .getDocument(
                GetDocumentRequest.newBuilder()
                    .setApp(required(app, "app"))
                    .setEnv(required(env, "env"))
                    .setNamespace(required(namespace, "namespace"))
                    .setConfigKey(required(configKey, "configKey"))
                    .build()
            )
            .document
            .toResponse()

    @POST
    @Path("/defaults")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(
        summary = "Create any of these documents the app does not have yet",
        description =
            "What a service calls on startup to declare the configuration it expects. Only the " +
                "missing documents are created — an existing one is left exactly as an operator " +
                "last edited it, which is what makes this safe to call on every boot.",
    )
    fun syncDefaults(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        body: SyncDefaultsBody?,
    ): SyncDefaultsResponse {
        val defaults = body?.defaults ?: throw InvalidRequestException("defaults is required.")
        val request =
            SyncDefaultsRequest.newBuilder()
                .setApp(required(app, "app"))
                .setEnv(required(env, "env"))
                .addAllDefaults(
                    defaults.map {
                        ConfigDefault.newBuilder()
                            .setNamespace(required(it.namespace, "namespace"))
                            .setConfigKey(required(it.configKey, "configKey"))
                            .setDefaultContentJson(
                                required(it.defaultContentJson, "defaultContentJson")
                            )
                            .build()
                    }
                )
                .build()

        val response = service.syncDefaults(request)
        return SyncDefaultsResponse(
            version = response.version,
            createdKeys =
                response.createdKeysList.map {
                    ConfigDocumentKeyResponse(namespace = it.namespace, configKey = it.configKey)
                },
        )
    }
}

internal fun required(value: String?, field: String): String =
    value?.takeIf { it.isNotBlank() } ?: throw InvalidRequestException("$field must not be empty.")

internal fun etag(version: Long): jakarta.ws.rs.core.EntityTag =
    jakarta.ws.rs.core.EntityTag(version.toString())

/**
 * `If-None-Match` may carry a weak marker and quotes, and a client that has never read a snapshot
 * sends `*`. Anything we cannot read as a version means "no known version" — a full snapshot is
 * always a correct answer, where guessing a number would not be.
 */
internal fun parseETag(header: String): Long? =
    header.trim().removePrefix("W/").trim('"').toLongOrNull()

internal fun GetSnapshotResponse.toSnapshot(): SnapshotResponse =
    SnapshotResponse(version = version, documents = documentsList.map { it.toResponse() })

internal fun gg.grounds.grpc.config.ConfigDocument.toResponse(): ConfigDocumentResponse =
    ConfigDocumentResponse(
        namespace = namespace,
        configKey = configKey,
        contentJson = contentJson,
        version = version,
    )
