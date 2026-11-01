package gg.grounds

import java.time.Duration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** Two real NATS clients on different brokers catch local-proxy-only delivery assumptions. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigNetworkDeliveryTest {
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
    fun `remote proxy receives change hint and missed events reconcile through snapshots`() {
        fixture.consumer(hub = false).use { local ->
            fixture.consumer(hub = true).use { remote ->
                val a = local.subscribe("config.delivery.pilot.changed")
                val b = remote.subscribe("config.delivery.pilot.changed")
                local.flush(Duration.ofSeconds(3))
                remote.flush(Duration.ofSeconds(3))
                Thread.sleep(300) // allow leaf interest propagation after both SUB flushes
                val written =
                    fixture
                        .request(
                            "PUT",
                            "/v1/config/admin/apps/delivery/envs/pilot/namespaces/test/documents/first",
                            mapOf("contentJson" to "{\"value\":1}"),
                        )
                        .expect(200)
                for (subscription in listOf(a, b)) {
                    val message = subscription.nextMessage(Duration.ofSeconds(5))
                    assertNotNull(message, "Both broker-local and remote proxies receive the hint")
                    val event = fixture.json.readTree(message!!.data)
                    assertEquals("delivery", event["app"].asText())
                    assertEquals("pilot", event["env"].asText())
                    assertEquals(written.json["version"].asLong(), event["version"].asLong())
                    assertTrue(event.has("configKey"))
                    assertFalse(event.has("config_key"))
                }
                val old =
                    fixture
                        .request(
                            "GET",
                            "/v1/config/apps/delivery/envs/pilot/snapshot",
                            token = fixture.token("proxy-b"),
                            instance = 1,
                        )
                        .expect(200)
                b.unsubscribe() // proxy-b misses this hint by design
                fixture
                    .request(
                        "PUT",
                        "/v1/config/admin/apps/delivery/envs/pilot/namespaces/test/documents/second",
                        mapOf("contentJson" to "{}"),
                        token = fixture.token("proxy-a"),
                    )
                    .expect(200)
                val polled =
                    fixture
                        .request(
                            "GET",
                            "/v1/config/apps/delivery/envs/pilot/snapshot",
                            token = fixture.token("proxy-b"),
                            instance = 1,
                            headers = mapOf("If-None-Match" to old.header("ETag")),
                        )
                        .expect(200)
                assertEquals(2, polled.json["documents"].size())
                assertTrue(polled.json["version"].asLong() > old.json["version"].asLong())
            }
        }
    }

    @Test
    fun `database remains authoritative during NATS outage and rotated token reconnects`() {
        fixture.stopLeaf()
        try {
            RuntimeFixture.await("NATS disconnect observed") { fixture.natsDown() }
            fixture
                .request(
                    "PUT",
                    "/v1/config/admin/apps/outage/envs/pilot/namespaces/test/documents/key",
                    mapOf("contentJson" to "{\"durable\":true}"),
                )
                .expect(200)
            val snapshot =
                fixture
                    .request("GET", "/v1/config/apps/outage/envs/pilot/snapshot", instance = 1)
                    .expect(200)
            assertEquals(
                fixture.json.readTree("{\"durable\":true}"),
                fixture.json.readTree(snapshot.json["documents"][0]["contentJson"].asText()),
            )
            fixture.request("GET", "/q/health/ready").expect(200)
        } finally {
            fixture.restartLeafWithRotatedToken()
        }
        RuntimeFixture.await("NATS reauthenticated") { !fixture.natsDown() }
        fixture.consumer(hub = true).use { remote ->
            val subscription = remote.subscribe("config.reconnected.pilot.changed")
            remote.flush(Duration.ofSeconds(3))
            Thread.sleep(300)
            fixture
                .request(
                    "PUT",
                    "/v1/config/admin/apps/reconnected/envs/pilot/namespaces/test/documents/key",
                    mapOf("contentJson" to "{}"),
                    instance = 1,
                )
                .expect(200)
            assertNotNull(subscription.nextMessage(Duration.ofSeconds(5)))
        }
    }

    @Test
    fun `database outage removes readiness and recovery restores it`() {
        fixture.stopDatabase()
        try {
            RuntimeFixture.await("DB failure visible in readiness") {
                try {
                    fixture.request("GET", "/q/health/ready").status == 503
                } catch (_: java.net.http.HttpTimeoutException) {
                    true
                } // a kubelet probe timeout is also not Ready
            }
        } finally {
            fixture.restartDatabase()
        }
        RuntimeFixture.await("DB recovery") {
            fixture.request("GET", "/q/health/ready").status == 200
        }
    }
}
