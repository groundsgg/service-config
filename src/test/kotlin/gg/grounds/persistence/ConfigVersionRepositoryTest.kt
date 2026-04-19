package gg.grounds.persistence

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

class ConfigVersionRepositoryTest {
    private val dataSource: DataSource = mock()
    private val connection: Connection = mock()
    private val statement: PreparedStatement = mock()
    private val resultSet: ResultSet = mock()
    private val repository = ConfigVersionRepository(dataSource)

    @Test
    fun `getVersion returns version when row exists`() {
        whenever(dataSource.connection).thenReturn(connection)
        whenever(connection.prepareStatement(any())).thenReturn(statement)
        whenever(statement.executeQuery()).thenReturn(resultSet)
        whenever(resultSet.next()).thenReturn(true)
        whenever(resultSet.getLong("version")).thenReturn(7L)

        val version = repository.getVersion("player", "prod")

        assertEquals(7L, version)
    }

    @Test
    fun `getVersion returns zero when version row does not exist`() {
        whenever(dataSource.connection).thenReturn(connection)
        whenever(connection.prepareStatement(any())).thenReturn(statement)
        whenever(statement.executeQuery()).thenReturn(resultSet)
        whenever(resultSet.next()).thenReturn(false)

        val version = repository.getVersion("player", "prod")

        assertEquals(0L, version)
    }

    @Test
    fun `getVersion rethrows SQLException when read fails`() {
        val sqlError = SQLException("database unavailable")
        whenever(dataSource.connection).thenThrow(sqlError)

        val thrown =
            assertThrows(SQLException::class.java) { repository.getVersion("player", "prod") }

        assertEquals(sqlError, thrown)
    }

    @Test
    fun `incrementVersion rethrows SQLException when write fails`() {
        val sqlError = SQLException("database unavailable")
        whenever(dataSource.connection).thenThrow(sqlError)

        val thrown =
            assertThrows(SQLException::class.java) { repository.incrementVersion("player", "prod") }

        assertEquals(sqlError, thrown)
    }
}
