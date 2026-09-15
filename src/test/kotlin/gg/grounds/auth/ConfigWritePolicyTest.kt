package gg.grounds.auth

import gg.grounds.api.ConfigRequestContexts
import io.smallrye.config.PropertiesConfigSource
import io.smallrye.config.SmallRyeConfigBuilder
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ConfigWritePolicyTest {

    private val policy = ConfigWritePolicy(":velocity=velocity, :velocity-2=velocity")

    @Test
    fun `a named writer may write the app it was named with`() {
        assertTrue(policy.mayWrite("system:serviceaccount:stage:velocity", "velocity"))
    }

    /** Both proxy releases share one MOTD, which is why the mapping is subject to app. */
    @Test
    fun `a second release may write the same app`() {
        assertTrue(policy.mayWrite("system:serviceaccount:stage:velocity-2", "velocity"))
    }

    /** The proxy that may set the MOTD may not rewrite another service's configuration. */
    @Test
    fun `a named writer may not write another app`() {
        assertFalse(policy.mayWrite("system:serviceaccount:stage:velocity", "service-player"))
    }

    @Test
    fun `an unnamed caller may write nothing`() {
        assertFalse(policy.mayWrite("system:serviceaccount:stage:default", "velocity"))
        assertFalse(policy.mayWrite("", "velocity"))
    }

    /** The same deployment exists in every region, so the namespace is deliberately not pinned. */
    @Test
    fun `matches in any namespace`() {
        assertTrue(policy.mayWrite("system:serviceaccount:games:velocity", "velocity"))
    }

    /** Defence against a prefix match: `velocity-3` must not inherit `velocity`'s grant. */
    @Test
    fun `does not match a longer service account name`() {
        assertFalse(policy.mayWrite("system:serviceaccount:stage:velocity-3", "velocity"))
    }

    @Test
    fun `unset means admin-only, which is what it was before`() {
        val none = ConfigWritePolicy("")

        assertEquals(0, none.writerCount())
        assertFalse(none.mayWrite("system:serviceaccount:stage:velocity", "velocity"))
    }

    @Test
    fun `unconfigured scoped writers resolve to an empty policy`() {
        val properties =
            Properties().apply {
                Files.newInputStream(Path.of("src/main/resources/application.properties"))
                    .use(::load)
            }
        val configured =
            SmallRyeConfigBuilder()
                .withSources(PropertiesConfigSource(properties, "application.properties"))
                .addDefaultInterceptors()
                .build()
                .getValue("grounds.auth.scoped-writers", String::class.java)

        assertEquals(0, ConfigWritePolicy(configured).writerCount())
    }

    /** One typo in a comma-separated list must not stop the service from starting. */
    @Test
    fun `drops entries that are not subject-suffix equals app`() {
        val sloppy = ConfigWritePolicy(":velocity=velocity,nonsense,=velocity,:orphan=")

        assertEquals(1, sloppy.writerCount())
        assertTrue(sloppy.mayWrite("system:serviceaccount:stage:velocity", "velocity"))
    }

    @Test
    fun `an exact writer may write only its configured document`() {
        val exact = ConfigWritePolicy(":forge=network/stage/resourcepacks/global")
        val document =
            ConfigRequestContexts.toDocumentContext("network", "stage", "resourcepacks", "global")

        assertTrue(exact.mayWriteAs("system:serviceaccount:games:forge", document))
        assertFalse(exact.mayWriteAs("system:serviceaccount:games:forge", "network"))
        assertFalse(
            exact.mayWriteAs(
                "system:serviceaccount:games:forge",
                ConfigRequestContexts.toDocumentContext("other", "stage", "resourcepacks", "global"),
            )
        )
        assertFalse(
            exact.mayWriteAs(
                "system:serviceaccount:games:forge",
                ConfigRequestContexts.toDocumentContext(
                    "network",
                    "prod",
                    "resourcepacks",
                    "global",
                ),
            )
        )
        assertFalse(
            exact.mayWriteAs(
                "system:serviceaccount:games:forge",
                ConfigRequestContexts.toDocumentContext("network", "stage", "other", "global"),
            )
        )
        assertFalse(
            exact.mayWriteAs(
                "system:serviceaccount:games:forge",
                ConfigRequestContexts.toDocumentContext(
                    "network",
                    "stage",
                    "resourcepacks",
                    "other",
                ),
            )
        )
    }

    @Test
    fun `legacy writers and admins retain document write access`() {
        val legacy = ConfigWritePolicy(":velocity=network")
        val document =
            ConfigRequestContexts.toDocumentContext("network", "stage", "resourcepacks", "global")

        assertTrue(legacy.mayWriteAs("system:serviceaccount:games:velocity", document))
        assertTrue(legacy.mayWriteAs("system:serviceaccount:platform-admin:config-admin", document))
    }

    @Test
    fun `malformed exact writer entries fail closed and valid duplicate wins last`() {
        val configured =
            ":forge=network/stage/resourcepacks/global," +
                ":two=network/stage,:three=network/stage/resourcepacks," +
                ":five=network/stage/resourcepacks/global/extra," +
                ":empty=network//resourcepacks/global," +
                ":invalid=network/stage/resourcepacks/global!," +
                ":forge=network/stage/resourcepacks/override"
        val policy = ConfigWritePolicy(configured)

        assertEquals(1, policy.writerCount())
        assertFalse(
            policy.mayWriteAs(
                "system:serviceaccount:games:forge",
                ConfigRequestContexts.toDocumentContext(
                    "network",
                    "stage",
                    "resourcepacks",
                    "global",
                ),
            )
        )
        assertTrue(
            policy.mayWriteAs(
                "system:serviceaccount:games:forge",
                ConfigRequestContexts.toDocumentContext(
                    "network",
                    "stage",
                    "resourcepacks",
                    "override",
                ),
            )
        )
    }
}
