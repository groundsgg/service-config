package gg.grounds.rest

import io.grpc.Status
import io.grpc.StatusRuntimeException
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider

/** Argument validation and authorisation, raised by the resources themselves. */
class InvalidRequestException(message: String) : RuntimeException(message)

@Provider
class InvalidRequestMapper : ExceptionMapper<InvalidRequestException> {
    override fun toResponse(exception: InvalidRequestException): Response =
        problem(400, "Invalid request", exception.message, "invalid_request")
}

class ForbiddenException(message: String) : RuntimeException(message)

@Provider
class ForbiddenMapper : ExceptionMapper<ForbiddenException> {
    override fun toResponse(exception: ForbiddenException): Response =
        problem(403, "Forbidden", exception.message, "forbidden")
}

/**
 * The document services signal failure with gRPC statuses, because that is what they were written
 * against and both facades call them unchanged. Translating here rather than rewriting them is
 * deliberate: a REST layer that re-derived "this key already exists" would be a second opinion on a
 * rule, and the two would eventually disagree.
 *
 * This mapper is the seam. It goes when the services stop throwing `StatusRuntimeException`, which
 * is the last thing gRPC leaves behind here.
 */
@Provider
class GrpcStatusMapper : ExceptionMapper<StatusRuntimeException> {
    override fun toResponse(exception: StatusRuntimeException): Response {
        val detail = exception.status.description
        return when (exception.status.code) {
            Status.Code.INVALID_ARGUMENT ->
                problem(400, "Invalid request", detail, "invalid_request")
            Status.Code.UNAUTHENTICATED ->
                problem(401, "Unauthenticated", detail, "unauthenticated")
            Status.Code.PERMISSION_DENIED -> problem(403, "Forbidden", detail, "forbidden")
            Status.Code.NOT_FOUND -> problem(404, "Not found", detail, "not_found")
            Status.Code.ALREADY_EXISTS -> problem(409, "Already exists", detail, "already_exists")
            // The document moved under the caller: they sent an expectedVersion that is no longer
            // current. 409 rather than 412, because the precondition is in the body, not a header.
            Status.Code.FAILED_PRECONDITION ->
                problem(409, "Version conflict", detail, "version_conflict")
            else -> problem(500, "Internal error", detail, "internal")
        }
    }
}

internal fun problem(status: Int, title: String, detail: String?, code: String): Response =
    Response.status(status)
        .type(ProblemDetails.PROBLEM_JSON)
        .entity(ProblemDetails(title = title, status = status, detail = detail, code = code))
        .build()
