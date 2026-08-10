package gg.grounds.rest

import org.eclipse.microprofile.openapi.annotations.media.Schema

@Schema(name = "ConfigDocument", description = "One configuration document.")
data class ConfigDocumentResponse(
    @get:Schema(description = "Grouping within the app, e.g. `motd`.") val namespace: String,
    @get:Schema(description = "The document's key within the namespace.") val configKey: String,
    @get:Schema(description = "The document itself, as a JSON string.") val contentJson: String,
    @get:Schema(description = "The document's own version, for optimistic writes.")
    val version: Long,
)

@Schema(
    name = "ConfigSnapshot",
    description = "Every document a caller should hold, at one version.",
)
data class SnapshotResponse(
    @get:Schema(
        description =
            "The snapshot's version. Send it back as `If-None-Match` and an unchanged " +
                "snapshot answers 304 with no body."
    )
    val version: Long,
    val documents: List<ConfigDocumentResponse>,
)

@Schema(name = "ConfigDefault", description = "A document to create if the app does not have it.")
data class ConfigDefaultBody(
    @get:Schema(required = true) val namespace: String?,
    @get:Schema(required = true) val configKey: String?,
    @get:Schema(description = "Must be valid JSON.", required = true)
    val defaultContentJson: String?,
)

@Schema(
    name = "SyncDefaultsRequest",
    description = "The documents an app expects to exist, sent on startup.",
)
data class SyncDefaultsBody(val defaults: List<ConfigDefaultBody>?)

@Schema(name = "ConfigDocumentKey", description = "Namespace and key of one document.")
data class ConfigDocumentKeyResponse(val namespace: String, val configKey: String)

@Schema(name = "SyncDefaultsResult", description = "What the sync created.")
data class SyncDefaultsResponse(
    @get:Schema(description = "The app's version after the sync.") val version: Long,
    @get:Schema(description = "Only the documents that did not exist before.")
    val createdKeys: List<ConfigDocumentKeyResponse>,
)

@Schema(name = "ConfigDocumentList", description = "Documents an operator asked to see.")
data class DocumentListResponse(val documents: List<ConfigDocumentResponse>)

@Schema(name = "CreateConfigDocument", description = "A document that must not exist yet.")
data class CreateDocumentBody(
    @get:Schema(required = true) val configKey: String?,
    @get:Schema(description = "Must be valid JSON.", required = true) val contentJson: String?,
    @get:Schema(description = "Who to record as the author.") val updatedBy: String? = null,
)

@Schema(name = "PutConfigDocument", description = "A document to create or replace.")
data class PutDocumentBody(
    @get:Schema(description = "Must be valid JSON.", required = true) val contentJson: String?,
    @get:Schema(description = "Who to record as the author.") val updatedBy: String? = null,
    @get:Schema(
        description =
            "The version the caller believes is current. Omitted writes unconditionally; a " +
                "mismatch is a 409 rather than a silent overwrite of somebody else's change."
    )
    val expectedVersion: Long? = null,
)

@Schema(name = "WriteResult", description = "The app's version after a write.")
data class WriteResultResponse(val version: Long)

@Schema(name = "DeleteResult", description = "Whether a document was there to delete.")
data class DeleteResultResponse(val deleted: Boolean, val version: Long)
