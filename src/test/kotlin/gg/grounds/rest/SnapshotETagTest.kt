package gg.grounds.rest

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

/**
 * `If-None-Match` is how a caller says which snapshot it already holds.
 *
 * Anything unreadable has to mean "no known version", because the fallback is a full snapshot — a
 * correct answer that costs a round trip. Guessing a number instead would answer 304 against a
 * version the caller never had, and the caller would sit on stale configuration indefinitely.
 */
class SnapshotETagTest {

    @Test
    fun `a quoted version is read`() {
        assertEquals(42L, parseETag("\"42\""))
    }

    @Test
    fun `a weak etag is read — the marker is not part of the version`() {
        assertEquals(42L, parseETag("W/\"42\""))
    }

    @Test
    fun `an unquoted version is read, since some clients send it bare`() {
        assertEquals(42L, parseETag("42"))
    }

    @Test
    fun `a wildcard means no known version, not "any version"`() {
        assertNull(parseETag("*"))
    }

    @Test
    fun `a list of etags is not a version we can act on`() {
        // RFC allows several. We hold exactly one version per snapshot, so this is not ours.
        assertNull(parseETag("\"1\", \"2\""))
    }

    @Test
    fun `nonsense is no version`() {
        assertNull(parseETag(""))
        assertNull(parseETag("\"not-a-number\""))
    }

    @Test
    fun `the tag we emit is the tag we can read back`() {
        assertEquals(7L, parseETag(etag(7L).toString()))
    }
}
