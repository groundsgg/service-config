package gg.grounds.domain

import java.time.Instant

data class ConfigDocument(
    val id: Long = 0,
    val app: String,
    val env: String,
    val namespace: String,
    val configKey: String,
    val contentJson: String,
    val version: Long = 1,
    val createdAt: Instant = Instant.now(),
    val updatedAt: Instant = Instant.now(),
    val updatedBy: String? = null,
)
