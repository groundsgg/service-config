package gg.grounds

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration

/** A private, file-based lifecycle bridge lets the Python driver reuse the tested fixture. */
object RuntimeFixtureMain {
    @JvmStatic
    fun main(args: Array<String>) {
        val image = requireNotNull(System.getProperty("grounds.runtime.image"))
        val path =
            Path.of(requireNotNull(System.getProperty("grounds.runtime.manifest"))).toAbsolutePath()
        val stop = path.resolveSibling(path.fileName.toString() + ".stop")
        Files.createDirectories(path.parent)
        Files.deleteIfExists(stop)
        RuntimeFixture.start(image).use { fixture ->
            val privateFile =
                Files.createTempFile(
                    path.parent,
                    "fixture-",
                    ".json",
                    PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rw-------")
                    ),
                )
            val info =
                mapOf(
                    "baseUrls" to
                        fixture.apps.map { "http://${it.host}:${it.getMappedPort(9000)}" },
                    "containerIds" to fixture.apps.map { it.containerId },
                    "readySeconds" to fixture.appReadySeconds,
                    "adminToken" to fixture.token("config-admin", expiresIn = 12 * 3600),
                    "dependencyImages" to
                        mapOf(
                            "postgres" to RuntimeFixture.POSTGRES,
                            "pooler" to RuntimeFixture.POOLER,
                            "nats" to RuntimeFixture.NATS,
                        ),
                )
            Files.writeString(privateFile, fixture.json.writeValueAsString(info))
            Files.move(privateFile, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            val end = System.nanoTime() + Duration.ofHours(4).toNanos()
            while (!Files.exists(stop) && System.nanoTime() < end) Thread.sleep(250)
        }
        Files.deleteIfExists(path)
        Files.deleteIfExists(stop)
    }
}
