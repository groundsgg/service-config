package gg.grounds.persistence

import gg.grounds.domain.ConfigDocument
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.sql.SQLException
import javax.sql.DataSource

@ApplicationScoped
class ConfigDocumentRepository
@Inject
constructor(
    private val readRepository: ConfigDocumentReadRepository,
    private val writeRepository: ConfigDocumentWriteRepository,
) {
    constructor(
        dataSource: DataSource
    ) : this(ConfigDocumentReadRepository(dataSource), ConfigDocumentWriteRepository(dataSource))

    data class DefaultConfig(
        val namespace: String,
        val configKey: String,
        val defaultContentJson: String,
    )

    sealed interface UpsertAndIncrementVersionResult {
        data class Updated(val version: Long) : UpsertAndIncrementVersionResult

        data class Failed(val cause: SQLException) : UpsertAndIncrementVersionResult
    }

    sealed interface DeleteAndIncrementVersionResult {
        data class Deleted(val version: Long) : DeleteAndIncrementVersionResult

        data object NotFound : DeleteAndIncrementVersionResult

        data class Failed(val cause: SQLException) : DeleteAndIncrementVersionResult
    }

    fun findAll(app: String, env: String): List<ConfigDocument> = readRepository.findAll(app, env)

    fun findByNamespace(app: String, env: String, namespace: String): List<ConfigDocument> =
        readRepository.findByNamespace(app, env, namespace)

    fun findOne(app: String, env: String, namespace: String, configKey: String): ConfigDocument? =
        readRepository.findOne(app, env, namespace, configKey)

    fun upsert(document: ConfigDocument): Boolean = writeRepository.upsert(document)

    fun upsertAndIncrementVersion(document: ConfigDocument): UpsertAndIncrementVersionResult =
        writeRepository.upsertAndIncrementVersion(document)

    fun insertIfNotExists(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
        defaultContentJson: String,
    ): Boolean =
        writeRepository.insertIfNotExists(app, env, namespace, configKey, defaultContentJson)

    fun insertIfNotExists(
        app: String,
        env: String,
        defaults: List<DefaultConfig>,
    ): List<DefaultConfig> = writeRepository.insertIfNotExists(app, env, defaults)

    fun delete(app: String, env: String, namespace: String, configKey: String): Boolean =
        writeRepository.delete(app, env, namespace, configKey)

    fun deleteAndIncrementVersion(
        app: String,
        env: String,
        namespace: String,
        configKey: String,
    ): DeleteAndIncrementVersionResult =
        writeRepository.deleteAndIncrementVersion(app, env, namespace, configKey)
}
