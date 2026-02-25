package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.math.BigDecimal;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for ResultSet operations. */
public class ResultSetIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      if (((DatabricksConnection) connection).getConnectionContext().getClientType()
              == DatabricksClientType.THRIFT
          && getFakeServiceMode() == FakeServiceExtension.FakeServiceMode.REPLAY) {
        // Hacky fix
        // Wiremock has error in stub matching for close operation in THRIFT + REPLAY mode
      } else {
        connection.close();
      }
    }
  }

  @Test
  void testRetrievalOfBasicDataTypes() throws SQLException {
    String tableName = "basic_data_types_table";
    setupDatabaseTable(connection, tableName);
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(connection, insertSQL);

    String query = "SELECT id, col1 FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);

    while (resultSet.next()) {
      assertEquals(1, resultSet.getInt("id"), "ID should be of type Integer and value 1");
      assertEquals(
          "value1", resultSet.getString("col1"), "col1 should be of type String and value value1");
    }
    deleteTable(connection, tableName);
  }

  @Test
  void testRetrievalOfComplexDataTypes() throws SQLException {
    String tableName = "complex_data_types_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "datetime_col TIMESTAMP, "
            + "decimal_col DECIMAL(10, 2), "
            + "date_col DATE"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, datetime_col, decimal_col, date_col) VALUES "
            + "(1, '2021-01-01 00:00:00', 123.45, '2021-01-01')";
    executeSQL(connection, insertSQL);

    String query =
        "SELECT datetime_col, decimal_col, date_col FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);
    while (resultSet.next()) {
      assertInstanceOf(
          Timestamp.class,
          resultSet.getTimestamp("datetime_col"),
          "datetime_col should be of type Timestamp");
      assertInstanceOf(
          BigDecimal.class,
          resultSet.getBigDecimal("decimal_col"),
          "decimal_col should be of type BigDecimal");
      assertInstanceOf(
          Date.class, resultSet.getDate("date_col"), "date_col should be of type Date");
    }
    deleteTable(connection, tableName);
  }

  @Test
  void testHandlingNullValues() throws SQLException {
    String tableName = "null_values_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "nullable_col VARCHAR(255)"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL = "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id) VALUES (1)";
    executeSQL(connection, insertSQL);

    String query = "SELECT nullable_col FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(connection, query);

    while (resultSet.next()) {
      String field = resultSet.getString("nullable_col");
      assertNull(field, "Field should be null when not set");
    }
    deleteTable(connection, tableName);
  }

  @Test
  void testNavigationInsideResultSet() throws SQLException {
    String tableName = "navigation_table";
    int numRows = 10; // Number of rows to insert and navigate through

    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    for (int i = 1; i <= numRows; i++) {
      String insertSQL =
          "INSERT INTO " + getFullyQualifiedTableName(tableName) + " (id) VALUES (" + i + ")";
      executeSQL(connection, insertSQL);
    }

    String query =
        "SELECT id FROM " + getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName;
    ResultSet resultSet = executeQuery(connection, query);

    int count = 0;
    try {
      while (resultSet.next()) {
        count++;
      }
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          e.printStackTrace(); // Log or handle the exception as needed
        }
      }
    }

    assertEquals(
        numRows,
        count,
        "Should have navigated through " + numRows + " rows, but navigated through " + count);
    deleteTable(connection, tableName);
  }

  // --- ResultSet navigation and position tests ---

  @Test
  void testForwardNavigation_MultipleRows() throws SQLException {
    String tableName = "nav_forward_table";
    setupDatabaseTable(connection, tableName);
    String fqn = getFullyQualifiedTableName(tableName);
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (3, 'e', 'f')");

    ResultSet rs = executeQuery(connection, "SELECT id FROM " + fqn + " ORDER BY id");
    assertNotNull(rs);

    int count = 0;
    while (rs.next()) {
      count++;
      assertEquals(count, rs.getInt("id"), "Row " + count + " should have id=" + count);
    }
    assertEquals(3, count, "Should navigate through all 3 rows");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testIsBeforeFirst_BeforeNavigation() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);
    assertTrue(rs.isBeforeFirst(), "Cursor should be before first row initially");

    rs.next();
    assertFalse(rs.isBeforeFirst(), "Cursor should not be before first after next()");
    rs.close();
  }

  @Test
  void testIsFirst_OnFirstRow() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);
    assertFalse(rs.isFirst(), "isFirst() should be false before calling next()");

    assertTrue(rs.next(), "Should have first row");
    assertTrue(rs.isFirst(), "isFirst() should be true on first row");
    rs.close();
  }

  @Test
  void testNextReturnsFalse_AfterExhausted() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num");
    assertNotNull(rs);

    assertTrue(rs.next(), "Should have first row");
    assertEquals(1, rs.getInt("num"));
    assertFalse(rs.next(), "Should have no more rows after last");
    rs.close();
  }

  @Test
  void testGetRow_TracksPosition() throws SQLException {
    String tableName = "nav_getrow_table";
    setupDatabaseTable(connection, tableName);
    String fqn = getFullyQualifiedTableName(tableName);
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (1, 'a', 'b')");
    executeSQL(connection, "INSERT INTO " + fqn + " (id, col1, col2) VALUES (2, 'c', 'd')");

    ResultSet rs = executeQuery(connection, "SELECT id FROM " + fqn + " ORDER BY id");
    assertNotNull(rs);

    assertEquals(0, rs.getRow(), "getRow() should return 0 before first row");

    assertTrue(rs.next());
    assertEquals(1, rs.getRow(), "getRow() should return 1 on first row");

    assertTrue(rs.next());
    assertEquals(2, rs.getRow(), "getRow() should return 2 on second row");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetType_ForwardOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.TYPE_FORWARD_ONLY, rs.getType(), "ResultSet type should be TYPE_FORWARD_ONLY");
    rs.close();
  }

  @Test
  void testResultSetConcurrency_ReadOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.CONCUR_READ_ONLY,
        rs.getConcurrency(),
        "ResultSet concurrency should be CONCUR_READ_ONLY");
    rs.close();
  }

  @Test
  void testFetchSize_DefaultsToZero() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(0, rs.getFetchSize(), "getFetchSize() should return 0 (unsupported)");
    rs.close();
  }

  @Test
  void testSetFetchSize_AcceptedWithWarning() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertDoesNotThrow(() -> rs.setFetchSize(100), "setFetchSize should not throw");
    rs.close();
  }

  @Test
  void testFetchDirection_ForwardOnly() throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT 1 AS num");

    assertEquals(
        ResultSet.FETCH_FORWARD, rs.getFetchDirection(), "Fetch direction should be FETCH_FORWARD");
    rs.close();
  }
}
