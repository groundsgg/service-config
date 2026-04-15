package gg.grounds.events

import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ConfigServiceReadinessCheckTest {
    private val objectMapper = ObjectMapper()

    @Test
    fun `call keeps readiness up when database is reachable and NATS is disconnected`() {
        val dataSource: DataSource = mock()
        val connection: Connection = mock()
        whenever(dataSource.connection).thenReturn(connection)
        whenever(connection.isValid(1)).thenReturn(true)
        val publisher = ConfigChangePublisher("nats://localhost:4222", objectMapper)

        val response = ConfigServiceReadinessCheck(dataSource, publisher).call()

        assertEquals("UP", response.status.toString())
    }

    @Test
    fun `call marks readiness down when database is unreachable`() {
        val dataSource: DataSource = mock()
        whenever(dataSource.connection).thenThrow(SQLException("database unavailable"))
        val publisher = ConfigChangePublisher("nats://localhost:4222", objectMapper)

        val response = ConfigServiceReadinessCheck(dataSource, publisher).call()

        assertEquals("DOWN", response.status.toString())
    }
}
