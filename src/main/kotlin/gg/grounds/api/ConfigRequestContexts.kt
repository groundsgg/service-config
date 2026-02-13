package gg.grounds.api

object ConfigRequestContexts {
    data class AppEnvContext(val app: String, val env: String)

    data class NamespaceContext(val app: String, val env: String, val namespace: String)

    data class DocumentContext(
        val app: String,
        val env: String,
        val namespace: String,
        val configKey: String,
    )

    fun toAppEnvContext(app: String, env: String): AppEnvContext {
        return AppEnvContext(app.trim(), env.trim())
    }

    fun toNamespaceContext(app: String, env: String, namespace: String): NamespaceContext {
        val context = toAppEnvContext(app, env)
        return NamespaceContext(context.app, context.env, namespace.trim())
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
            configKey = configKey.trim(),
        )
    }
}
