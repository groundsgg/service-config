package gg.grounds.auth

import gg.grounds.api.ConfigRequestContexts
import jakarta.enterprise.context.ApplicationScoped
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger

/**
 * Who may change a config document, on top of [AuthGuard]'s admin service accounts.
 *
 * The admin rule alone says a caller is trusted with *every* app's config or with none, and a
 * ServiceAccount's name is what grants it. That is fine for an operator's account and wrong for a
 * service that configures itself: the Velocity proxies own the network MOTD, and making them
 * `config-admin` to write one document would hand them every other app's configuration as well. It
 * is also not something a pod can opt into — a projected token always carries the pod's own SA, so
 * "be admin for this one write" is not available.
 *
 * So writers are named explicitly, by the deployment, as `<subject-suffix>=<app>` or for a
 * least-privilege document owner, `<subject-suffix>=<app>/<env>/<namespace>/<configKey>`:
 * ```
 * GROUNDS_CONFIG_WRITERS=":velocity=velocity,:velocity-2=velocity"
 * ```
 *
 * Both proxies may write the `velocity` app and nothing else. Two entries rather than one because
 * the two releases run under two ServiceAccounts and share one document — which is the point of the
 * mapping being subject→app rather than subject→itself.
 *
 * Suffix matching, and so namespace-agnostic, exactly like [AuthGuard]: the same deployment exists
 * in every region, and pinning the namespace would mean an entry per region that nobody would keep
 * in step. Reads are unaffected — `ConfigService` is open to any authenticated caller.
 */
@ApplicationScoped
class ConfigWritePolicy(
    @param:ConfigProperty(name = "grounds.auth.scoped-writers", defaultValue = "")
    private val configured: String
) {

    /** Subject suffix → the app or exact document it may write. */
    private val writers: Map<String, Grant> by lazy { parse(configured) }

    /**
     * May [subject] write [app]? Admins may write anything; a scoped writer only the app it was
     * named with.
     */
    fun mayWriteAs(subject: String, app: String): Boolean =
        AuthGuard.isAdminSubject(subject) || mayWrite(subject, app)

    /**
     * May [subject] write [document]? App writers retain their existing authority across that
     * app; an exact writer is limited to the normalized document identity it was configured for.
     */
    fun mayWriteAs(subject: String, document: ConfigRequestContexts.DocumentContext): Boolean =
        AuthGuard.isAdminSubject(subject) ||
            writers.any { (suffix, grant) ->
                subject.endsWith(suffix) &&
                    when (grant) {
                        is Grant.App -> grant.app == document.app
                        is Grant.Document -> grant.document == document
                    }
            }

    /** Visible for testing. */
    internal fun mayWrite(subject: String, app: String): Boolean =
        writers.any { (suffix, grant) ->
            subject.endsWith(suffix) && grant is Grant.App && grant.app == app
        }

    /** Visible for testing. */
    internal fun writerCount(): Int = writers.size

    private fun parse(raw: String): Map<String, Grant> {
        val parsed = mutableMapOf<String, Grant>()
        raw.split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .forEach { entry ->
                val suffix = entry.substringBefore('=', "").trim()
                val allowed = entry.substringAfter('=', "").trim()
                if (suffix.isEmpty() || allowed.isEmpty()) {
                    // Dropped rather than fatal: one typo in a comma-separated list must not stop
                    // the service from starting, and a writer that is silently missing shows up as
                    // a PERMISSION_DENIED with the caller's subject in it.
                    LOG.warnf(
                        "Ignoring config writer entry '%s' — expected <subject-suffix>=<app>",
                        entry,
                    )
                    return@forEach
                }
                val grant = parseGrant(allowed) ?: run {
                    LOG.warnf(
                        "Ignoring config writer entry '%s' — expected <subject-suffix>=<app> or <subject-suffix>=<app>/<env>/<namespace>/<configKey>",
                        entry,
                    )
                    return@forEach
                }
                parsed.put(suffix, grant)?.let {
                    LOG.warnf("Config writer '%s' listed twice; keeping last valid grant", suffix)
                }
            }
        if (parsed.isNotEmpty()) {
            LOG.infof("Scoped config writers: %s", parsed)
        }
        return parsed
    }

    private fun parseGrant(allowed: String): Grant? {
        val segments = allowed.split('/')
        return when (segments.size) {
            1 -> Grant.App(allowed)
            4 ->
                try {
                    Grant.Document(
                        ConfigRequestContexts.toDocumentContext(
                            segments[0],
                            segments[1],
                            segments[2],
                            segments[3],
                        )
                    )
                } catch (_: RuntimeException) {
                    null
                }
            else -> null
        }
    }

    private sealed interface Grant {
        data class App(val app: String) : Grant

        data class Document(val document: ConfigRequestContexts.DocumentContext) : Grant
    }

    private companion object {
        val LOG: Logger = Logger.getLogger(ConfigWritePolicy::class.java)
    }
}
