package gg.grounds.domain

/**
 * Why a config operation was refused.
 *
 * The document services used to say this with `io.grpc.Status`, which meant the rules travelled
 * with a transport: dropping gRPC would have taken "this key already exists" with it. These are the
 * same distinctions, owned by the domain, and the HTTP layer is what turns them into status codes.
 */
enum class ConfigErrorCode {
    /** Malformed input — an empty segment, a body that is not JSON. */
    INVALID_ARGUMENT,
    /** No such document. */
    NOT_FOUND,
    /** A create against a key that is already taken. */
    ALREADY_EXISTS,
    /** The caller's `expectedVersion` is no longer current — somebody else wrote first. */
    VERSION_CONFLICT,
    /** The caller is authenticated but not allowed to do this. */
    PERMISSION_DENIED,
    /** Ours, not the caller's. */
    INTERNAL,
}

/** A refused config operation. [message] is written for a human and is safe to hand back. */
class ConfigException(val code: ConfigErrorCode, message: String) : RuntimeException(message)
