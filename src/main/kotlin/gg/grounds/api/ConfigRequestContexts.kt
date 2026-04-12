package gg.grounds.api

import io.grpc.Status

object ConfigRequestContexts {
    data class AppEnvContext(val app: String, val env: String)

    data class NamespaceContext(val app: String, val env: String, val namespace: String)

    data class DocumentContext(
        val app: String,
        val env: String,
        val namespace: String,
        val configKey: String,
    )

    fun requireSegment(fieldName: String, value: String): String {
        return normalizeRequiredSegment(fieldName, value)
    }

    fun toAppEnvContext(app: String, env: String): AppEnvContext {
        return AppEnvContext(
            normalizeRequiredSegment("app", app),
            normalizeRequiredSegment("env", env),
        )
    }

    fun toNamespaceContext(
        app: String,
        env: String,
        namespace: String,
        allowEmptyNamespace: Boolean = false,
    ): NamespaceContext {
        val context = toAppEnvContext(app, env)
        return NamespaceContext(
            context.app,
            context.env,
            normalizeSegment("namespace", namespace, allowEmpty = allowEmptyNamespace),
        )
    }

    fun toDocumentContext(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
    ): DocumentContext {
        val namespaceContext = toNamespaceContext(app, env, namespace)
        return DocumentContext(
            app = namespaceContext.app,
            env = namespaceContext.env,
            namespace = namespaceContext.namespace,
            configKey = normalizeRequiredSegment("configKey", configKey),
        )
    }

    private fun normalizeRequiredSegment(fieldName: String, value: String): String {
        return normalizeSegment(fieldName, value, allowEmpty = false)
    }

    private fun normalizeSegment(fieldName: String, value: String, allowEmpty: Boolean): String {
        val normalizedValue = value.trim()
        if (normalizedValue.isEmpty()) {
            if (allowEmpty) {
                return normalizedValue
            }
            throw invalidArgument("$fieldName must not be blank")
        }
        if (!SEGMENT_PATTERN.matches(normalizedValue)) {
            throw invalidArgument("$fieldName must match ${SEGMENT_PATTERN_DESCRIPTION}")
        }
        return normalizedValue
    }

    private fun invalidArgument(message: String) =
        Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException()

    private val SEGMENT_PATTERN = Regex("[A-Za-z0-9_-]+")
    private const val SEGMENT_PATTERN_DESCRIPTION = "[A-Za-z0-9_-]+"
}
