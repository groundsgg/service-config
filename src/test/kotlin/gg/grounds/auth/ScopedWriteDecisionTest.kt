package gg.grounds.auth

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The write decision as the HTTP layer asks it: subject in, verdict out, no transport involved.
 *
 * The gRPC facade asks the same question through a gRPC Context, so this is the one place the two
 * could drift — and the reason it takes a subject rather than reading one.
 */
class ScopedWriteDecisionTest {

    private val policy = ConfigWritePolicy(":velocity=velocity,:velocity-2=velocity")

    @Test
    fun `an admin may write any app`() {
        assertTrue(
            policy.mayWriteAs("system:serviceaccount:platform-admin:config-admin", "velocity")
        )
        assertTrue(
            policy.mayWriteAs(
                "system:serviceaccount:platform-admin:platform-admin",
                "service-player",
            )
        )
    }

    @Test
    fun `a named writer may write the app it was named with`() {
        assertTrue(policy.mayWriteAs("system:serviceaccount:stage:velocity", "velocity"))
        // Two ServiceAccounts, one shared document — which is why the mapping is subject to app
        // rather than subject to itself.
        assertTrue(policy.mayWriteAs("system:serviceaccount:stage:velocity-2", "velocity"))
    }

    @Test
    fun `a named writer may not write anything else`() {
        // The proxy that owns the network MOTD must not be able to rewrite service-player's config.
        assertFalse(policy.mayWriteAs("system:serviceaccount:stage:velocity", "service-player"))
    }

    @Test
    fun `an unnamed workload may write nothing`() {
        assertFalse(policy.mayWriteAs("system:serviceaccount:stage:default", "velocity"))
    }

    @Test
    fun `the local-development subject is not a writer`() {
        // Auth off must not silently grant writes, or the check is never exercised until prod.
        assertFalse(policy.mayWriteAs("local-development", "velocity"))
    }
}
