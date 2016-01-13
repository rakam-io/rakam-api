import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestingDatabase {

    @Test
    public void testName() throws Exception {
        try (TestingPostgreSqlServer server = new TestingPostgreSqlServer("testuser", "testdb")) {
            assertEquals(server.getUser(), "testuser");
            assertEquals(server.getDatabase(), "testdb");
            assertEquals(server.getJdbcUrl().substring(0, 5), "jdbc:");
            assertEquals(server.getPort(), URI.create(server.getJdbcUrl().substring(5)).getPort());

            try (Connection connection = DriverManager.getConnection(server.getJdbcUrl())) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE test_table (c1 bigint PRIMARY KEY)");
                    statement.execute("INSERT INTO test_table (c1) VALUES (1)");
                    try (ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM test_table")) {
                        assertTrue(resultSet.next());
                        assertEquals(resultSet.getLong(1), 1L);
                        assertFalse(resultSet.next());
                    }
                }
            }
        }
    }
}
