package gg.grounds.auth

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

    /** One typo in a comma-separated list must not stop the service from starting. */
    @Test
    fun `drops entries that are not subject-suffix equals app`() {
        val sloppy = ConfigWritePolicy(":velocity=velocity,nonsense,=velocity,:orphan=")

        assertEquals(1, sloppy.writerCount())
        assertTrue(sloppy.mayWrite("system:serviceaccount:stage:velocity", "velocity"))
    }
}
