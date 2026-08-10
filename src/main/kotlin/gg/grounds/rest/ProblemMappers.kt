package gg.grounds.rest

import gg.grounds.domain.ConfigErrorCode
import gg.grounds.domain.ConfigException
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
 * The document services raise [ConfigException] and the resources translate nothing: a REST layer
 * that re-derived "this key already exists" would be a second opinion on a rule, and the two would
 * eventually disagree. The mapping from a refusal to a status code lives here and nowhere else.
 */
@Provider
class ConfigExceptionMapper : ExceptionMapper<ConfigException> {
    override fun toResponse(exception: ConfigException): Response =
        when (exception.code) {
            ConfigErrorCode.INVALID_ARGUMENT ->
                problem(400, "Invalid request", exception.message, "invalid_request")
            ConfigErrorCode.PERMISSION_DENIED ->
                problem(403, "Forbidden", exception.message, "forbidden")
            ConfigErrorCode.NOT_FOUND -> problem(404, "Not found", exception.message, "not_found")
            ConfigErrorCode.ALREADY_EXISTS ->
                problem(409, "Already exists", exception.message, "already_exists")
            // The document moved under the caller: they sent an expectedVersion that is no longer
            // current. 409 rather than 412, because the precondition is in the body, not a header.
            ConfigErrorCode.VERSION_CONFLICT ->
                problem(409, "Version conflict", exception.message, "version_conflict")
            ConfigErrorCode.INTERNAL ->
                problem(500, "Internal error", exception.message, "internal")
        }
}

internal fun problem(status: Int, title: String, detail: String?, code: String): Response =
    Response.status(status)
        .type(ProblemDetails.PROBLEM_JSON)
        .entity(ProblemDetails(title = title, status = status, detail = detail, code = code))
        .build()
