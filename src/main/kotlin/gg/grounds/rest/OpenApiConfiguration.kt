package gg.grounds.rest

import jakarta.ws.rs.ApplicationPath
import jakarta.ws.rs.core.Application
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType
import org.eclipse.microprofile.openapi.annotations.info.Info
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme

@ApplicationPath("/")
@OpenAPIDefinition(
    info =
        Info(
            title = "Config API",
            version = "1.0.0",
            description =
                "Versioned configuration documents, addressed as (app, env, namespace, key).\n\n" +
                    "A service declares the documents it expects on startup and then reads a " +
                    "snapshot; only the missing documents are created, so an operator's edit " +
                    "survives every redeploy. Snapshots carry a version as an `ETag`, and a " +
                    "caller that sends it back gets a 304 when nothing has moved.\n\n" +
                    "Changes are also announced on NATS, so a service learns that its " +
                    "configuration moved without polling for it. The event carries the version, " +
                    "not the documents — the reader comes back here for those.\n\n" +
                    "Reading is open to any authenticated workload. Writing is not: browsing and " +
                    "creating are admin-only, while replacing and deleting one app's documents " +
                    "can be granted to that app's own ServiceAccount.",
        )
)
@SecurityScheme(
    securitySchemeName = "bearerAuth",
    type = SecuritySchemeType.HTTP,
    scheme = "bearer",
    bearerFormat = "JWT",
    description =
        "The projected ServiceAccount token from /var/run/secrets/grounds/token, with the " +
            "grounds-services audience.",
)
class OpenApiConfiguration : Application()
