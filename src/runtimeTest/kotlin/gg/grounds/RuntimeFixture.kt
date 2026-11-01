package gg.grounds

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jose.jwk.JWKSet
import com.nimbusds.jose.jwk.RSAKey
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT
import com.sun.net.httpserver.HttpsConfigurator
import com.sun.net.httpserver.HttpsServer
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.time.Duration
import java.time.Instant
import java.util.Date
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import org.testcontainers.Testcontainers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import org.testcontainers.utility.MountableFile

/** All dependencies and credentials are disposable and belong only to this fixture. */
class RuntimeFixture private constructor(private val image: String) : AutoCloseable {
    val json = ObjectMapper()
    private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()
    private val network = Network.newNetwork()
    private val containers = mutableListOf<Container>()
    private val temp = Files.createTempDirectory("config-runtime-")
    private val issuerExecutor = Executors.newCachedThreadPool()
    private var server: HttpsServer? = null
    @Volatile private var key: RSAKey = signingKey()
    @Volatile private var expectedBearer = "jwks-a"
    private var leafToken = "runtime-token-a"
    val jwksFetchCount = AtomicInteger()
    val rotatedBearerSeen = AtomicBoolean()
    lateinit var postgres: Container
        private set

    lateinit var pooler: Container
        private set

    lateinit var hub: Container
        private set

    lateinit var leaf: Container
        private set

    val apps = mutableListOf<Container>()
    val appReadySeconds = mutableListOf<Double>()

    class Container(image: String) : GenericContainer<Container>(DockerImageName.parse(image))

    data class Response(
        val status: Int,
        val body: String,
        val headers: java.net.http.HttpHeaders,
        val json: JsonNode,
    ) {
        fun expect(expected: Int): Response {
            check(status == expected) { "Expected HTTP $expected, got $status: ${body.take(1000)}" }
            return this
        }

        fun header(name: String): String = headers.firstValue(name).orElseThrow()
    }

    fun token(
        subject: String,
        audience: String = "grounds-services",
        expiresIn: Long = 3600,
    ): String {
        val signing = key
        val claims =
            JWTClaimsSet.Builder()
                .subject("system:serviceaccount:pilot:$subject")
                .audience(audience)
                .issuer("https://runtime-fixture")
                .expirationTime(Date.from(Instant.now().plusSeconds(expiresIn)))
                .build()
        return SignedJWT(JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signing.keyID).build(), claims)
            .apply { sign(RSASSASigner(signing)) }
            .serialize()
    }

    fun request(
        method: String,
        path: String,
        body: Any? = null,
        token: String = token("config-admin"),
        instance: Int = 0,
        headers: Map<String, String> = emptyMap(),
    ): Response {
        val uri =
            URI.create("http://${apps[instance].host}:${apps[instance].getMappedPort(9000)}$path")
        val builder = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(8))
        if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
        headers.forEach { (k, v) -> builder.header(k, v) }
        if (body != null) builder.header("Content-Type", "application/json")
        val response =
            client.send(
                builder
                    .method(
                        method,
                        body?.let {
                            HttpRequest.BodyPublishers.ofString(json.writeValueAsString(it))
                        } ?: HttpRequest.BodyPublishers.noBody(),
                    )
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        return Response(
            response.statusCode(),
            response.body(),
            response.headers(),
            if (response.body().isBlank()) json.nullNode() else json.readTree(response.body()),
        )
    }

    private fun remember(container: Container): Container = container.also { containers.add(it) }

    private fun configureIssuer() {
        fun openssl(vararg args: String) {
            val process =
                ProcessBuilder(listOf("openssl") + args)
                    .directory(temp.toFile())
                    .redirectErrorStream(true)
                    .start()
            val output = process.inputStream.bufferedReader().readText()
            check(process.waitFor() == 0) { "Test TLS setup failed: $output" }
        }
        openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.crt",
            "-days",
            "2",
            "-subj",
            "/CN=Config Runtime Test CA",
        )
        openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "server.key",
            "-out",
            "server.csr",
            "-subj",
            "/CN=host.testcontainers.internal",
        )
        Files.writeString(
            temp.resolve("extensions.cnf"),
            "subjectAltName=DNS:host.testcontainers.internal\nextendedKeyUsage=serverAuth\n",
        )
        openssl(
            "x509",
            "-req",
            "-in",
            "server.csr",
            "-CA",
            "ca.crt",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "server.crt",
            "-days",
            "2",
            "-extfile",
            "extensions.cnf",
        )
        openssl(
            "pkcs12",
            "-export",
            "-out",
            "server.p12",
            "-inkey",
            "server.key",
            "-in",
            "server.crt",
            "-certfile",
            "ca.crt",
            "-passout",
            "pass:fixture",
        )
        val store =
            KeyStore.getInstance("PKCS12").apply {
                Files.newInputStream(temp.resolve("server.p12")).use {
                    load(it, "fixture".toCharArray())
                }
            }
        val kmf =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply {
                init(store, "fixture".toCharArray())
            }
        val ssl = SSLContext.getInstance("TLS").apply { init(kmf.keyManagers, null, null) }
        server =
            HttpsServer.create(InetSocketAddress("0.0.0.0", 0), 0).apply {
                httpsConfigurator = HttpsConfigurator(ssl)
                executor = issuerExecutor
                createContext("/jwks") { exchange ->
                    val authorization = exchange.requestHeaders.getFirst("Authorization")
                    val valid =
                        authorization == "Bearer $expectedBearer" &&
                            exchange.requestHeaders.getFirst("Accept") == "application/jwk-set+json"
                    val body =
                        if (valid) JWKSet(key.toPublicJWK()).toString()
                        else "Unauthorized fixture JWKS fetch"
                    if (valid) {
                        jwksFetchCount.incrementAndGet()
                        if (expectedBearer == "jwks-b") rotatedBearerSeen.set(true)
                    }
                    exchange.responseHeaders.add("Content-Type", "application/jwk-set+json")
                    val bytes = body.toByteArray()
                    exchange.sendResponseHeaders(if (valid) 200 else 401, bytes.size.toLong())
                    exchange.responseBody.use { it.write(bytes) }
                    exchange.close()
                }
                start()
            }
        Testcontainers.exposeHostPorts(server!!.address.port)
    }

    private fun startContainers() {
        configureIssuer()
        postgres =
            remember(
                Container(POSTGRES)
                    .withNetwork(network)
                    .withNetworkAliases("postgres")
                    .withEnv("POSTGRES_DB", "pilot")
                    .withEnv("POSTGRES_USER", "app")
                    .withEnv("POSTGRES_PASSWORD", "fixture-password")
                    .waitingFor(
                        Wait.forLogMessage(
                            ".*database system is ready to accept connections.*\\n",
                            2,
                        )
                    )
            )
        postgres.start()
        pooler =
            remember(
                Container(POOLER)
                    .withNetwork(network)
                    .withNetworkAliases("pooler")
                    .withEnv("DB_HOST", "postgres")
                    .withEnv("DB_NAME", "pilot")
                    .withEnv("DB_USER", "app")
                    .withEnv("DB_PASSWORD", "fixture-password")
                    .withEnv("AUTH_TYPE", "scram-sha-256")
                    .withEnv("POOL_MODE", "transaction")
                    .withEnv("ADMIN_USERS", "app")
                    .withExposedPorts(5432)
                    .waitingFor(Wait.forListeningPort())
            )
        pooler.start()
        hub =
            remember(
                Container(NATS)
                    .withNetwork(network)
                    .withNetworkAliases("hub")
                    .withExposedPorts(4222)
                    .withCopyToContainer(
                        org.testcontainers.images.builder.Transferable.of(hubConfig()),
                        "/etc/nats/pilot.conf",
                    )
                    .withCommand("-c", "/etc/nats/pilot.conf")
                    .waitingFor(Wait.forListeningPort())
            )
        hub.start()
        leaf =
            remember(
                Container(NATS)
                    .withNetwork(network)
                    .withNetworkAliases("leaf")
                    .withExposedPorts(4222)
                    .withCopyToContainer(
                        org.testcontainers.images.builder.Transferable.of(leafConfig()),
                        "/etc/nats/pilot.conf",
                    )
                    .withCommand("-c", "/etc/nats/pilot.conf")
                    .waitingFor(Wait.forListeningPort())
            )
        leaf.start()
        Files.writeString(temp.resolve("jwks-token"), expectedBearer)
        Files.writeString(temp.resolve("nats-token"), leafToken)
        repeat(2) {
            apps +=
                remember(
                    Container(image)
                        .withNetwork(network)
                        .withExposedPorts(9000)
                        .withCopyFileToContainer(
                            MountableFile.forHostPath(temp.resolve("ca.crt"), 292),
                            "/fixture/ca.crt",
                        )
                        .withCopyFileToContainer(
                            MountableFile.forHostPath(temp.resolve("jwks-token"), 292),
                            "/fixture/jwks-token",
                        )
                        .withCopyFileToContainer(
                            MountableFile.forHostPath(temp.resolve("nats-token"), 292),
                            "/fixture/nats-token",
                        )
                        .withEnv(
                            "POSTGRES_URL",
                            "jdbc:postgresql://pooler:5432/pilot?currentSchema=config",
                        )
                        .withEnv(
                            "POSTGRES_DIRECT_URL",
                            "jdbc:postgresql://postgres:5432/pilot?currentSchema=config",
                        )
                        .withEnv("POSTGRES_USER", "app")
                        .withEnv("POSTGRES_PASSWORD", "fixture-password")
                        .withEnv("QUARKUS_DATASOURCE_JDBC_MAX_SIZE", "2")
                        .withEnv("QUARKUS_DATASOURCE_JDBC_MIN_SIZE", "1")
                        .withEnv("QUARKUS_DATASOURCE_JDBC_INITIAL_SIZE", "1")
                        .withEnv("QUARKUS_DATASOURCE_JDBC_ACQUISITION_TIMEOUT", "2s")
                        .withEnv("NATS_URL", "nats://leaf:4222")
                        .withEnv("NATS_RECONNECT_WAIT_SECONDS", "1")
                        .withEnv("GROUNDS_TOKEN_FILE", "/fixture/nats-token")
                        .withEnv("GROUNDS_AUTH_ENABLED", "true")
                        .withEnv(
                            "GROUNDS_AUTH_JWKS_URL",
                            "https://host.testcontainers.internal:${server!!.address.port}/jwks",
                        )
                        .withEnv("GROUNDS_AUTH_K8S_CA_FILE", "/fixture/ca.crt")
                        .withEnv("GROUNDS_AUTH_K8S_TOKEN_FILE", "/fixture/jwks-token")
                        .withEnv(
                            "GROUNDS_CONFIG_WRITERS",
                            ":proxy-a=scoped|delivery,:proxy-b=scoped|delivery",
                        )
                        .withEnv("OTEL_SDK_DISABLED", "true")
                        .withCreateContainerCmdModifier { cmd ->
                            cmd.hostConfig!!
                                .withMemory(768L * 1024 * 1024)
                                .withNanoCPUs(1_000_000_000L)
                        }
                        .waitingFor(
                            Wait.forHttp("/q/health/ready")
                                .forPort(9000)
                                .forStatusCode(200)
                                .withStartupTimeout(Duration.ofSeconds(120))
                        )
                )
        }
        System.getProperty("grounds.runtime.java-options")
            ?.takeIf { it.isNotBlank() }
            ?.let { options -> apps.forEach { it.withEnv("JAVA_TOOL_OPTIONS", options) } }
        val starts = Executors.newFixedThreadPool(2)
        try {
            apps
                .map { app ->
                    starts.submit<Double> {
                        val start = System.nanoTime()
                        app.start()
                        (System.nanoTime() - start) / 1e9
                    }
                }
                .forEach { appReadySeconds += it.get() }
        } finally {
            starts.shutdownNow()
        }
    }

    fun consumer(hub: Boolean): Connection {
        val broker = if (hub) this.hub else leaf
        return Nats.connect(
            Options.Builder()
                .server("nats://${broker.host}:${broker.getMappedPort(4222)}")
                .token((if (hub) "runtime-token-a" else leafToken).toCharArray())
                .connectionTimeout(Duration.ofSeconds(5))
                .build()
        )
    }

    fun rotateJwksAndBearer() {
        expectedBearer = "jwks-b"
        Files.writeString(temp.resolve("jwks-token"), expectedBearer)
        apps.forEach {
            it.copyFileToContainer(
                MountableFile.forHostPath(temp.resolve("jwks-token"), 292),
                "/fixture/jwks-token",
            )
        }
        key = signingKey()
    }

    fun stopLeaf() {
        leaf.dockerClient.stopContainerCmd(leaf.containerId).withTimeout(1).exec()
    }

    fun restartLeafWithRotatedToken() {
        leafToken = "runtime-token-b"
        Files.writeString(temp.resolve("nats-token"), leafToken)
        apps.forEach {
            it.copyFileToContainer(
                MountableFile.forHostPath(temp.resolve("nats-token"), 292),
                "/fixture/nats-token",
            )
        }
        leaf.copyFileToContainer(
            org.testcontainers.images.builder.Transferable.of(leafConfig()),
            "/etc/nats/pilot.conf",
        )
        leaf.dockerClient.startContainerCmd(leaf.containerId).exec()
    }

    fun natsDown(): Boolean {
        val ready = request("GET", "/q/health/ready").expect(200)
        return ready.json["checks"].any { it.path("data").path("nats").asText() == "down" }
    }

    fun stopDatabase() {
        postgres.dockerClient.stopContainerCmd(postgres.containerId).withTimeout(1).exec()
    }

    fun restartDatabase() {
        postgres.dockerClient.startContainerCmd(postgres.containerId).exec()
    }

    fun poolerConnections(): Int {
        val result =
            pooler.execInContainer(
                "psql",
                "postgresql://app:fixture-password@127.0.0.1:5432/pgbouncer",
                "-Atc",
                "SHOW CLIENTS",
            )
        check(result.exitCode == 0) { result.stderr }
        return result.stdout.lines().count { it.contains("|pilot|") }
    }

    private fun hubConfig() =
        "port: 4222\nauthorization { token: \"runtime-token-a\" }\nleafnodes { port: 7422, authorization { user: \"leaf\", password: \"fixture-leaf\" } }\n"

    private fun leafConfig() =
        "port: 4222\nauthorization { token: \"$leafToken\" }\nleafnodes { remotes: [ { url: \"nats://leaf:fixture-leaf@hub:7422\" } ] }\n"

    override fun close() {
        containers.asReversed().forEach { container ->
            runCatching {
                val log =
                    Path.of(
                        "build",
                        "runtime-fixture-logs",
                        container.containerId ?: UUID.randomUUID().toString(),
                    )
                Files.createDirectories(log.parent)
                if (container.containerId != null)
                    Files.writeString(
                        log.resolveSibling(log.fileName.toString() + ".log"),
                        container.logs,
                    )
                container.close()
            }
        }
        server?.stop(0)
        issuerExecutor.shutdownNow()
        network.close()
        Files.walk(temp).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
        }
    }

    companion object {
        const val POSTGRES =
            "postgres@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b"
        const val POOLER =
            "edoburu/pgbouncer@sha256:8638aa563607256d3c7440437534195f1066a92b68015d89ae9d6b935b444a6d"
        const val NATS =
            "nats@sha256:71092f77d707a4a81b12aca5096d6b2d2e07ad16aa57c84066940a17af74f61a"

        private fun signingKey() =
            RSAKeyGenerator(2048).keyID(UUID.randomUUID().toString()).generate()

        fun start(image: String): RuntimeFixture =
            RuntimeFixture(image).also { fixture ->
                try {
                    fixture.startContainers()
                } catch (error: Throwable) {
                    fixture.close()
                    throw error
                }
            }

        fun await(
            description: String,
            timeout: Duration = Duration.ofSeconds(90),
            condition: () -> Boolean,
        ) {
            val end = System.nanoTime() + timeout.toNanos()
            var last: Throwable? = null
            while (System.nanoTime() < end) {
                try {
                    if (condition()) return
                } catch (error: Exception) {
                    last = error
                }
                Thread.sleep(250)
            }
            throw AssertionError("Timed out waiting for $description", last)
        }
    }
}
