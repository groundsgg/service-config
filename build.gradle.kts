plugins {
    id("gg.grounds.root") version "0.1.1"
    id("gg.grounds.push") version "0.13.0"
    id("io.quarkus") version "3.38.0"
}

repositories {
    mavenLocal()
    mavenCentral()
    maven {
        url = uri("https://maven.pkg.github.com/groundsgg/*")
        credentials {
            username = providers.gradleProperty("github.user").get()
            password = providers.gradleProperty("github.token").get()
        }
    }
}

// The OpenAPI snapshot published to groundsgg/api-reference. Quarkus appends to
// whatever is already in build/generated/openapi, so a stale file would ship
// endpoints that no longer exist; clearing first makes the snapshot a statement
// about this commit.
val cleanProductionOpenApi =
    tasks.register<Delete>("cleanProductionOpenApi") {
        delete(layout.buildDirectory.dir("generated/openapi"))
        delete(layout.buildDirectory.dir("quarkus"))
        delete(layout.buildDirectory.dir("quarkus-app"))
        delete(layout.buildDirectory.dir("quarkus-build"))
    }

tasks
    .matching { it.name.startsWith("quarkus") }
    .configureEach { mustRunAfter(cleanProductionOpenApi) }

tasks.register<Copy>("generateOpenApiSnapshot") {
    group = "documentation"
    dependsOn(cleanProductionOpenApi, tasks.named("quarkusBuild"))
    from(layout.buildDirectory.file("generated/openapi/openapi.json"))
    into(layout.buildDirectory.dir("api-reference"))
    rename { "openapi.json" }
}

dependencies {
    implementation(enforcedPlatform("io.quarkus.platform:quarkus-bom:3.38.0"))
    implementation("io.quarkus:quarkus-arc")
    // The public API. HTTP is the only transport now.
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")
    implementation("io.quarkus:quarkus-smallrye-openapi")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    // No gRPC is served any more. The extension stays only because it owns the
    // protoc codegen behind `quarkus.generate-code.grpc.scan-for-proto`, and the
    // document services still pass the generated messages around as internal
    // request/response types. Both go when those take domain types instead —
    // a refactor of the write paths, and worth doing on its own.
    implementation("io.quarkus:quarkus-grpc")
    implementation("io.quarkus:quarkus-jackson")
    implementation("io.quarkus:quarkus-jdbc-postgresql")
    implementation("io.quarkus:quarkus-flyway")
    implementation("io.quarkus:quarkus-kotlin")
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("gg.grounds:library-grpc-contracts-config:0.2.0")
    implementation("io.nats:jnats:2.26.2")
    // JWT validation for incoming gRPC calls (v2.2 Service Architecture).
    implementation("com.nimbusds:nimbus-jose-jwt:10.9.1")
    // OpenTelemetry — server-side gRPC spans → Alloy → Tempo.
    implementation("io.quarkus:quarkus-opentelemetry")
    // Prometheus metrics on /q/metrics — JVM, HTTP and the Agroal pool.
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")

    compileOnly("com.google.protobuf:protobuf-kotlin")

    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.quarkus:quarkus-junit5-mockito")
    testImplementation("org.mockito.kotlin:mockito-kotlin:6.3.0")
    testImplementation("com.google.protobuf:protobuf-kotlin")
}

sourceSets { main { java { srcDirs("build/classes/java/quarkus-generated-sources/grpc") } } }

tasks
    .matching { it.name == "kaptGenerateStubsKotlin" }
    .configureEach {
        dependsOn("quarkusGenerateCode")
        dependsOn("quarkusGenerateCodeDev")
    }

tasks.processResources {
    val projectVersion = version.toString()
    filesMatching("**/default_banner.txt") {
        filter { line: String -> line.replace("@VERSION@", projectVersion) }
    }
}
