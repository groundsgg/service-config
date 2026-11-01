package gg.grounds

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** A missing native DTO/resource, bypassed auth or broken conditional write fails real requests. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigRuntimeContractTest {
    private lateinit var fixture: RuntimeFixture

    @BeforeAll
    fun start() {
        fixture = RuntimeFixture.start(System.getProperty("grounds.runtime.image"))
    }

    @AfterAll
    fun close() {
        if (::fixture.isInitialized) fixture.close()
    }

    @Test
    fun `CRUD and conditional snapshots preserve document and app versions`() {
        val app = "contract"
        val root = "/v1/config/admin/apps/$app/envs/pilot/namespaces/settings/documents"
        fixture
            .request(
                "POST",
                root,
                mapOf("configKey" to "hello", "contentJson" to "{\"enabled\":true}"),
            )
            .expect(200)
        fixture
            .request("POST", root, mapOf("configKey" to "hello", "contentJson" to "{}"))
            .expect(409)
        val document = fixture.request("GET", "$root/hello").expect(200)
        assertEquals(
            fixture.json.readTree("{\"enabled\":true}"),
            fixture.json.readTree(document.json["contentJson"].asText()),
        )
        val tag = document.header("ETag")
        assertEquals("\"${document.json["version"].asLong()}\"", tag)
        val updated =
            fixture
                .request(
                    "PUT",
                    "$root/hello",
                    mapOf("contentJson" to "{\"enabled\":false}"),
                    headers = mapOf("If-Match" to tag),
                )
                .expect(200)
        assertNotEquals(tag, updated.header("ETag"))
        fixture
            .request(
                "PUT",
                "$root/hello",
                mapOf("contentJson" to "{}"),
                headers = mapOf("If-Match" to tag),
            )
            .expect(409)
        fixture
            .request(
                "PUT",
                "$root/hello",
                mapOf("contentJson" to "{}", "expectedVersion" to 1),
                headers = mapOf("If-Match" to tag),
            )
            .expect(400)
        fixture.request("PUT", "$root/hello", mapOf("contentJson" to "not JSON")).expect(400)
        fixture
            .request(
                "PUT",
                "$root/hello",
                mapOf("contentJson" to "{}"),
                headers = mapOf("If-Match" to "W/\"1\""),
            )
            .expect(400)
        val snapshot =
            fixture.request("GET", "/v1/config/apps/$app/envs/pilot/snapshot").expect(200)
        assertEquals(1, snapshot.json["documents"].size())
        val unchanged =
            fixture
                .request(
                    "GET",
                    "/v1/config/apps/$app/envs/pilot/snapshot",
                    headers = mapOf("If-None-Match" to snapshot.header("ETag")),
                )
                .expect(304)
        assertEquals("", unchanged.body)
        fixture.request("DELETE", "$root/hello").expect(200)
        fixture.request("GET", "$root/hello").expect(404)
    }

    @Test
    fun `defaults are idempotent and survive reads through the other instance`() {
        val body =
            mapOf(
                "defaults" to
                    listOf(
                        mapOf(
                            "namespace" to "flags",
                            "configKey" to "test",
                            "defaultContentJson" to "{\"value\":1}",
                        )
                    )
            )
        val first =
            fixture
                .request("POST", "/v1/config/apps/defaults/envs/pilot/defaults", body)
                .expect(200)
        assertEquals(1, first.json["createdKeys"].size())
        val repeated =
            fixture
                .request("POST", "/v1/config/apps/defaults/envs/pilot/defaults", body, instance = 1)
                .expect(200)
        assertEquals(0, repeated.json["createdKeys"].size())
        assertEquals(first.json["version"].asLong(), repeated.json["version"].asLong())
    }

    @Test
    fun `JWT signatures audience expiry and scoped writer boundaries are enforced`() {
        val path = "/v1/config/apps/auth/envs/pilot/snapshot"
        fixture.request("GET", path, token = "").expect(401)
        fixture
            .request("GET", path, token = fixture.token("reader", audience = "wrong"))
            .expect(401)
        fixture.request("GET", path, token = fixture.token("reader", expiresIn = -600)).expect(401)
        fixture.request("GET", path, token = fixture.token("reader") + "broken").expect(401)
        fixture.request("GET", path, token = fixture.token("reader")).expect(200)
        val own = "/v1/config/admin/apps/scoped/envs/pilot/namespaces/flags/documents/a"
        fixture
            .request("PUT", own, mapOf("contentJson" to "{}"), token = fixture.token("proxy-a"))
            .expect(200)
        val denied =
            fixture
                .request(
                    "PUT",
                    own.replace("/scoped/", "/foreign/"),
                    mapOf("contentJson" to "{}"),
                    token = fixture.token("proxy-a"),
                )
                .expect(403)
        assertEquals("forbidden", denied.json["code"].asText())
    }

    @Test
    fun `HTTPS JWKS refresh trusts the mounted CA and rereads the projected bearer`() {
        fixture.rotateJwksAndBearer()
        RuntimeFixture.await("new JWKS key accepted") {
            fixture
                .request(
                    "GET",
                    "/v1/config/apps/rotation/envs/pilot/snapshot",
                    token = fixture.token("reader"),
                )
                .status == 200
        }
        assertTrue(
            fixture.jwksFetchCount.get() >= 3,
            "Both instances warm JWKS and at least one refreshes",
        )
        assertTrue(
            fixture.rotatedBearerSeen.get(),
            "JWKS refresh must use the replacement projected token",
        )
    }

    @Test
    fun `runtime queries use transaction pooling and both instances see committed writes`() {
        val path = "/v1/config/admin/apps/pooling/envs/pilot/namespaces/test/documents/key"
        fixture
            .request("PUT", path, mapOf("contentJson" to "{\"pooled\":true}"), instance = 0)
            .expect(200)
        assertEquals(
            fixture.json.readTree("{\"pooled\":true}"),
            fixture.json.readTree(
                fixture.request("GET", path, instance = 1).expect(200).json["contentJson"].asText()
            ),
        )
        assertTrue(
            fixture.poolerConnections() > 0,
            "runtime must actually connect through PgBouncer",
        )
    }
}
