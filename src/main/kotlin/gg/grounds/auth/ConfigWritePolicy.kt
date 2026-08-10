package gg.grounds.auth

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
 * So writers are named explicitly, by the deployment, as `<subject-suffix>=<app>`:
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

    /** Subject suffix → the one app it may write. */
    private val writers: Map<String, String> by lazy { parse(configured) }

    /**
     * May [subject] write [app]? Admins may write anything; a scoped writer only the app it was
     * named with.
     */
    fun mayWriteAs(subject: String, app: String): Boolean =
        AuthGuard.isAdminSubject(subject) || mayWrite(subject, app)

    /** Visible for testing. */
    internal fun mayWrite(subject: String, app: String): Boolean =
        writers.any { (suffix, allowedApp) -> subject.endsWith(suffix) && allowedApp == app }

    /** Visible for testing. */
    internal fun writerCount(): Int = writers.size

    private fun parse(raw: String): Map<String, String> {
        val parsed = mutableMapOf<String, String>()
        raw.split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .forEach { entry ->
                val suffix = entry.substringBefore('=', "").trim()
                val app = entry.substringAfter('=', "").trim()
                if (suffix.isEmpty() || app.isEmpty()) {
                    // Dropped rather than fatal: one typo in a comma-separated list must not stop
                    // the service from starting, and a writer that is silently missing shows up as
                    // a PERMISSION_DENIED with the caller's subject in it.
                    LOG.warnf(
                        "Ignoring config writer entry '%s' — expected <subject-suffix>=<app>",
                        entry,
                    )
                    return@forEach
                }
                parsed.put(suffix, app)?.let {
                    LOG.warnf("Config writer '%s' listed twice; keeping app '%s'", suffix, app)
                }
            }
        if (parsed.isNotEmpty()) {
            LOG.infof("Scoped config writers: %s", parsed)
        }
        return parsed
    }

    private companion object {
        val LOG: Logger = Logger.getLogger(ConfigWritePolicy::class.java)
    }
}
