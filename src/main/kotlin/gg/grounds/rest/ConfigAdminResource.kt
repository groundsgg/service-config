package gg.grounds.rest

import gg.grounds.api.ConfigAdminDocumentService
import gg.grounds.api.ConfigRequestContexts
import gg.grounds.auth.AuthGuard
import gg.grounds.auth.ConfigWritePolicy
import gg.grounds.grpc.config.CreateDocumentRequest
import gg.grounds.grpc.config.DeleteDocumentRequest
import gg.grounds.grpc.config.GetDocumentRequest
import gg.grounds.grpc.config.ListDocumentsRequest
import gg.grounds.grpc.config.PutDocumentRequest
import jakarta.inject.Inject
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.POST
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.Context
import jakarta.ws.rs.core.HttpHeaders
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.SecurityContext
import org.eclipse.microprofile.openapi.annotations.Operation
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.headers.Header
import org.eclipse.microprofile.openapi.annotations.media.Content
import org.eclipse.microprofile.openapi.annotations.media.Schema
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse
import org.eclipse.microprofile.openapi.annotations.tags.Tag

/**
 * Editing config documents, and browsing them across apps.
 *
 * Two different grants, on purpose. Browsing and creating are admin-only: seeing every app's
 * configuration is an operator's job, and *which* documents exist in an app is a shape decision.
 * Replacing and deleting one document additionally accept a writer named for that app or exact
 * document — which lets a service own one document without being handed every other app's
 * configuration along with it.
 *
 * The decisions themselves live in [AuthGuard] and [ConfigWritePolicy], taking the subject this
 * resource reads off the request; the gRPC facade asks the same functions through a gRPC Context.
 */
@Path("/v1/config/admin/apps/{app}/envs/{env}")
@Tag(name = "Config admin", description = "Editing configuration documents.")
@Produces(MediaType.APPLICATION_JSON)
class ConfigAdminResource
@Inject
constructor(
    private val service: ConfigAdminDocumentService,
    private val writePolicy: ConfigWritePolicy,
) {

    @GET
    @Path("/documents")
    @Operation(
        summary = "List an app's documents",
        description = "Admin only. `namespace` narrows the listing; omitted lists the whole app.",
    )
    @APIResponse(responseCode = "403", description = "The caller is not an admin ServiceAccount.")
    fun list(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @QueryParam("namespace") @DefaultValue("") namespace: String,
        @Context security: SecurityContext,
    ): DocumentListResponse {
        requireAdmin(security, "list documents")
        val response =
            service.listDocuments(
                ListDocumentsRequest.newBuilder()
                    .setApp(required(app, "app"))
                    .setEnv(required(env, "env"))
                    .setNamespace(namespace)
                    .build()
            )
        return DocumentListResponse(response.documentsList.map { it.toResponse() })
    }

    @GET
    @Path("/namespaces/{namespace}/documents/{configKey}")
    @Operation(
        summary = "Read any document",
        description =
            "Admin only. The response carries a strong `ETag` holding this document's version.",
    )
    @APIResponse(
        responseCode = "200",
        description = "The document and its version ETag.",
        content = [Content(schema = Schema(implementation = ConfigDocumentResponse::class))],
        headers =
            [
                Header(
                    name = "ETag",
                    description = "Strong ETag containing the document's version.",
                    schema = Schema(type = SchemaType.STRING),
                )
            ],
    )
    @APIResponse(responseCode = "404", description = "No such document.")
    fun get(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @PathParam("namespace") namespace: String?,
        @PathParam("configKey") configKey: String?,
        @Context security: SecurityContext,
    ): Response {
        requireAdmin(security, "read document")
        val document =
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
        return Response.ok(document).tag(etag(document.version)).build()
    }

    @POST
    @Path("/namespaces/{namespace}/documents")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(
        summary = "Create a document that does not exist yet",
        description =
            "Admin only — which documents an app has is a shape decision. A service that wants " +
                "create-or-replace wants PUT instead.",
    )
    @APIResponse(responseCode = "409", description = "The document already exists.")
    fun create(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @PathParam("namespace") namespace: String?,
        body: CreateDocumentBody?,
        @Context security: SecurityContext,
    ): WriteResultResponse {
        requireAdmin(security, "create document")
        val payload = body ?: throw InvalidRequestException("A request body is required.")
        val response =
            service.createDocument(
                CreateDocumentRequest.newBuilder()
                    .setApp(required(app, "app"))
                    .setEnv(required(env, "env"))
                    .setNamespace(required(namespace, "namespace"))
                    .setConfigKey(required(payload.configKey, "configKey"))
                    .setContentJson(required(payload.contentJson, "contentJson"))
                    .setUpdatedBy(payload.updatedBy ?: subjectOf(security))
                    .build()
            )
        return WriteResultResponse(response.version)
    }

    @PUT
    @Path("/namespaces/{namespace}/documents/{configKey}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(
        summary = "Create or replace a document",
        description =
            "Admin, an app writer, or an exact-document writer. Send either `If-Match` with one " +
                "strong document ETag or `expectedVersion` in the body to make the write conditional; " +
                "a mismatch answers 409 rather than quietly overwriting somebody else's change.",
    )
    @APIResponse(
        responseCode = "200",
        description = "The write result and its new version ETag.",
        content = [Content(schema = Schema(implementation = WriteResultResponse::class))],
        headers =
            [
                Header(
                    name = "ETag",
                    description = "Strong ETag containing the document's new version.",
                    schema = Schema(type = SchemaType.STRING),
                )
            ],
    )
    @APIResponse(
        responseCode = "409",
        description = "The body expectedVersion or If-Match version is no longer current.",
    )
    @APIResponse(
        responseCode = "400",
        description = "If-Match is invalid or conflicts with body expectedVersion.",
    )
    fun put(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @PathParam("namespace") namespace: String?,
        @PathParam("configKey") configKey: String?,
        body: PutDocumentBody?,
        @Context security: SecurityContext,
        @HeaderParam("If-Match") ifMatch: String? = null,
        @Context headers: HttpHeaders? = null,
    ): Response {
        val context =
            ConfigRequestContexts.toDocumentContext(
                required(app, "app"),
                required(env, "env"),
                required(namespace, "namespace"),
                required(configKey, "configKey"),
            )
        requireWrite(security, context, "replace document")
        val payload = body ?: throw InvalidRequestException("A request body is required.")
        val effectiveIfMatch = resolveIfMatch(ifMatch, headers)
        if (payload.expectedVersion != null && effectiveIfMatch != null) {
            throw InvalidRequestException("If-Match and expectedVersion must not be sent together.")
        }
        val builder =
            PutDocumentRequest.newBuilder()
                .setApp(context.app)
                .setEnv(context.env)
                .setNamespace(context.namespace)
                .setConfigKey(context.configKey)
                .setContentJson(required(payload.contentJson, "contentJson"))
                .setUpdatedBy(payload.updatedBy ?: subjectOf(security))
        (payload.expectedVersion ?: effectiveIfMatch?.let(::parseIfMatch))?.let {
            builder.expectedVersion = it
        }
        val response = service.putDocumentWithVersion(builder.build())
        return Response.ok(WriteResultResponse(response.appVersion))
            .tag(etag(response.documentVersion))
            .build()
    }

    @DELETE
    @Path("/namespaces/{namespace}/documents/{configKey}")
    @Operation(
        summary = "Delete a document",
        description = "Admin, an app writer, or an exact-document writer.",
    )
    fun delete(
        @PathParam("app") app: String?,
        @PathParam("env") env: String?,
        @PathParam("namespace") namespace: String?,
        @PathParam("configKey") configKey: String?,
        @Context security: SecurityContext,
    ): DeleteResultResponse {
        val context =
            ConfigRequestContexts.toDocumentContext(
                required(app, "app"),
                required(env, "env"),
                required(namespace, "namespace"),
                required(configKey, "configKey"),
            )
        requireWrite(security, context, "delete document")
        val response =
            service.deleteDocument(
                DeleteDocumentRequest.newBuilder()
                    .setApp(context.app)
                    .setEnv(context.env)
                    .setNamespace(context.namespace)
                    .setConfigKey(context.configKey)
                    .setDeletedBy(subjectOf(security))
                    .build()
            )
        return DeleteResultResponse(deleted = response.deleted, version = response.version)
    }

    private fun requireAdmin(security: SecurityContext, operation: String) {
        val subject = subjectOf(security)
        if (!AuthGuard.isAdminSubject(subject)) {
            throw ForbiddenException("$operation requires admin (caller=$subject)")
        }
    }

    private fun requireWrite(
        security: SecurityContext,
        document: ConfigRequestContexts.DocumentContext,
        operation: String,
    ) {
        val subject = subjectOf(security)
        if (!writePolicy.mayWriteAs(subject, document)) {
            throw ForbiddenException(
                "$operation on app '${document.app}' requires admin or a configured writer (caller=$subject)"
            )
        }
    }

    private fun subjectOf(security: SecurityContext): String =
        security.userPrincipal?.name.orEmpty()

    private fun resolveIfMatch(ifMatch: String?, headers: HttpHeaders?): String? {
        if (headers == null) return ifMatch
        val values = headers.getRequestHeader("If-Match") ?: emptyList()
        if (values.isEmpty()) return null
        if (values.size != 1 || values.single().isEmpty()) {
            throw InvalidRequestException(
                "If-Match must contain exactly one non-empty field value."
            )
        }
        return values.single()
    }
}
