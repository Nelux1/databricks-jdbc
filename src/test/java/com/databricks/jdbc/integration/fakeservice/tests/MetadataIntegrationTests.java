package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.SESSION_PATH;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for metadata retrieval. */
public class MetadataIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    try {
      connection = getValidJDBCConnection();
    } catch (SQLException e) {
      connection = null;
    }
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
  void testDatabaseMetadataRetrieval() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Basic database information
    assertFalse(
        metaData.getDatabaseProductName().isEmpty(), "Database product name should not be empty");
    assertFalse(
        metaData.getDatabaseProductVersion().isEmpty(),
        "Database product version should not be empty");
    assertFalse(metaData.getDriverName().isEmpty(), "Driver name should not be empty");
    assertFalse(metaData.getUserName().isEmpty(), "Username should not be empty");

    // Capabilities of the database
    assertTrue(
        metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY),
        "Database should support TYPE_FORWARD_ONLY ResultSet");

    // Limits imposed by the database (0 refers to infinite)
    assertTrue(metaData.getMaxConnections() >= 0, "Max connections should be greater than 0");
    assertTrue(
        metaData.getMaxTableNameLength() >= 0, "Max table name length should be greater than 0");
    assertTrue(
        metaData.getMaxColumnsInTable() >= 0, "Max columns in table should be greater than 0");

    if (isSqlExecSdkClient()) {
      // Create session request is sent
      getDatabricksApiExtension().verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));
    }
  }

  @Test
  void testResultSetMetadataRetrieval() throws SQLException {
    String tableName = "resultset_metadata_test_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT PRIMARY KEY, "
            + "name VARCHAR(255), "
            + "age INT"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, name, age) VALUES (1, 'Madhav', 24)";
    executeSQL(connection, insertSQL);

    String query = "SELECT id, name, age FROM " + getFullyQualifiedTableName(tableName);

    ResultSet resultSet = executeQuery(connection, query);
    assert resultSet != null;
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    // Check the number of columns
    int expectedColumnCount = 3;
    assertEquals(
        expectedColumnCount, resultSetMetaData.getColumnCount(), "Expected column count mismatch");

    // Check metadata for each column
    assertEquals("id", resultSetMetaData.getColumnName(1), "First column should be id");
    assertEquals(
        Types.INTEGER, resultSetMetaData.getColumnType(1), "id column should be of type INTEGER");

    assertEquals("name", resultSetMetaData.getColumnName(2), "Second column should be name");
    assertEquals(
        Types.VARCHAR, resultSetMetaData.getColumnType(2), "name column should be of type VARCHAR");

    assertEquals("age", resultSetMetaData.getColumnName(3), "Third column should be age");
    assertEquals(
        Types.INTEGER, resultSetMetaData.getColumnType(3), "age column should be of type INTEGER");

    // Additional checks for column properties
    for (int i = 1; i <= expectedColumnCount; i++) {
      assertEquals(
          ResultSetMetaData.columnNullable,
          resultSetMetaData.isNullable(i),
          "Column " + i + " should be nullable");
    }
    String SQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(connection, SQL);

    if (isSqlExecSdkClient()) {
      // At least 5 statement requests are sent: drop, create, insert, select, drop
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  @Test
  void testCatalogAndSchemaInformation() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    // Test getCatalogs
    try (ResultSet catalogs = metaData.getCatalogs()) {
      assertTrue(catalogs.next(), "There should be at least one catalog");
      do {
        String catalogName = catalogs.getString("TABLE_CAT");
        assertNotNull(catalogName, "Catalog name should not be null");
      } while (catalogs.next());
    }

    // Test getSchemas
    try (ResultSet schemas = metaData.getSchemas("main", "jdbc%")) {
      assertTrue(schemas.next(), "There should be at least one schema");
      do {
        String schemaName = schemas.getString("TABLE_SCHEM");
        assertNotNull(schemaName, "Schema name should not be null");
      } while (schemas.next());
    }

    // Verify tables retrieval with specific catalog and schema
    String catalog = "main";
    String schemaPattern = "jdbc_test_schema";
    String tableName = "catalog_and_schema_test_table";
    setupDatabaseTable(connection, tableName);
    try (ResultSet tables = metaData.getTables(catalog, schemaPattern, "%", null)) {
      assertTrue(
          tables.next(), "There should be at least one table in the specified catalog and schema");
      do {
        String fetchedTableName = tables.getString("TABLE_NAME");
        assertNotNull(fetchedTableName, "Table name should not be null");
      } while (tables.next());
    }

    // Test to get particular table
    try (ResultSet tables = metaData.getTables(catalog, schemaPattern, tableName, null)) {
      assertTrue(
          tables.next(), "There should be at least one table in the specified catalog and schema");
      do {
        String fetchedTableName = tables.getString("TABLE_NAME");
        assertEquals(
            tableName, fetchedTableName, "Table name should match the specified table name");
      } while (tables.next());
    }
    deleteTable(connection, tableName);

    if (isSqlExecSdkClient()) {
      // At least 7 statement requests are sent:
      // show catalogs, show schemas, drop table, create table, show tables, show particular table,
      // drop
      getDatabricksApiExtension()
          .verify(
              new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 7),
              postRequestedFor(urlEqualTo(STATEMENT_PATH)));
    }
  }

  // --- ResultSetMetaData property tests ---

  @Test
  void testResultSetMetaData_ColumnLabelAndTypeName() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS my_number, 'hello' AS my_text");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals(2, rsmd.getColumnCount());

    // Column labels should match the aliases
    assertEquals("my_number", rsmd.getColumnLabel(1), "Column label should match alias");
    assertEquals("my_text", rsmd.getColumnLabel(2), "Column label should match alias");

    // Column type names should be non-empty
    assertNotNull(rsmd.getColumnTypeName(1), "Column type name should not be null");
    assertFalse(rsmd.getColumnTypeName(1).isEmpty(), "Column type name should not be empty");
    assertNotNull(rsmd.getColumnTypeName(2), "Column type name should not be null");

    rs.close();
  }

  @Test
  void testResultSetMetaData_PrecisionAndScale() throws SQLException {
    String tableName = "metadata_precision_scale_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "id INT, "
            + "price DECIMAL(10, 2), "
            + "quantity BIGINT"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, price, quantity) VALUES (1, 99.95, 1000)";
    executeSQL(connection, insertSQL);

    ResultSet rs =
        executeQuery(
            connection, "SELECT id, price, quantity FROM " + getFullyQualifiedTableName(tableName));
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // DECIMAL(10, 2) should have precision=10, scale=2
    assertEquals(10, rsmd.getPrecision(2), "DECIMAL precision should be 10");
    assertEquals(2, rsmd.getScale(2), "DECIMAL scale should be 2");

    // INT and BIGINT should have scale=0
    assertEquals(0, rsmd.getScale(1), "INT scale should be 0");
    assertEquals(0, rsmd.getScale(3), "BIGINT scale should be 0");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetMetaData_ColumnDisplaySize() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num, 'hello' AS str");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // Display size should be > 0 for all columns
    assertTrue(rsmd.getColumnDisplaySize(1) > 0, "Display size for INT should be > 0");
    assertTrue(rsmd.getColumnDisplaySize(2) > 0, "Display size for STRING should be > 0");

    rs.close();
  }

  @Test
  void testResultSetMetaData_ColumnClassName() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num, 'hello' AS str, true AS flag");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // Each column should have a non-null, non-empty class name
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      String className = rsmd.getColumnClassName(i);
      assertNotNull(className, "Column " + i + " class name should not be null");
      assertFalse(className.isEmpty(), "Column " + i + " class name should not be empty");
    }

    rs.close();
  }

  @Test
  void testResultSetMetaData_BooleanProperties() throws SQLException {
    ResultSet rs = executeQuery(connection, "SELECT 1 AS num, 'hello' AS str");
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      // isAutoIncrement should return false (Databricks doesn't support auto-increment)
      assertFalse(rsmd.isAutoIncrement(i), "Column " + i + " should not be auto-increment");

      // isSearchable should return true for standard query results
      assertTrue(rsmd.isSearchable(i), "Column " + i + " should be searchable");

      // isCurrency should return false
      assertFalse(rsmd.isCurrency(i), "Column " + i + " should not be currency");

      // isReadOnly should return true (ResultSet is read-only)
      assertTrue(rsmd.isReadOnly(i), "Column " + i + " should be read-only");
    }

    rs.close();
  }

  @Test
  void testResultSetMetaData_SignedProperty() throws SQLException {
    String tableName = "metadata_signed_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "int_col INT, "
            + "str_col STRING, "
            + "bool_col BOOLEAN"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (int_col, str_col, bool_col) VALUES (42, 'text', true)";
    executeSQL(connection, insertSQL);

    ResultSet rs =
        executeQuery(
            connection,
            "SELECT int_col, str_col, bool_col FROM " + getFullyQualifiedTableName(tableName));
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    // INT should be signed
    assertTrue(rsmd.isSigned(1), "INT column should be signed");
    // STRING should not be signed
    assertFalse(rsmd.isSigned(2), "STRING column should not be signed");

    rs.close();
    deleteTable(connection, tableName);
  }

  @Test
  void testResultSetMetaData_MultipleDataTypes() throws SQLException {
    String tableName = "metadata_multitypes_table";
    String createTableSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " ("
            + "tinyint_col TINYINT, "
            + "smallint_col SMALLINT, "
            + "int_col INT, "
            + "bigint_col BIGINT, "
            + "float_col FLOAT, "
            + "double_col DOUBLE, "
            + "string_col STRING, "
            + "bool_col BOOLEAN, "
            + "date_col DATE, "
            + "ts_col TIMESTAMP"
            + ");";
    setupDatabaseTable(connection, tableName, createTableSQL);

    ResultSet rs =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertNotNull(rs);
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals(10, rsmd.getColumnCount(), "Should have 10 columns");

    // Verify each column has a valid SQL type
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      int sqlType = rsmd.getColumnType(i);
      String typeName = rsmd.getColumnTypeName(i);
      assertNotNull(typeName, "Column " + i + " type name should not be null");
      assertTrue(sqlType != 0, "Column " + i + " should have a non-zero SQL type");
    }

    // Verify specific type mappings
    assertEquals("tinyint_col", rsmd.getColumnName(1));
    assertEquals("smallint_col", rsmd.getColumnName(2));
    assertEquals("int_col", rsmd.getColumnName(3));
    assertEquals("bigint_col", rsmd.getColumnName(4));
    assertEquals("float_col", rsmd.getColumnName(5));
    assertEquals("double_col", rsmd.getColumnName(6));
    assertEquals("string_col", rsmd.getColumnName(7));
    assertEquals("bool_col", rsmd.getColumnName(8));
    assertEquals("date_col", rsmd.getColumnName(9));
    assertEquals("ts_col", rsmd.getColumnName(10));

    rs.close();
    deleteTable(connection, tableName);
  }

  // --- DatabaseMetaData.getTypeInfo() test ---

  @Test
  void testGetTypeInfo_ReturnsTypeCatalog() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet typeInfo = metaData.getTypeInfo();
    assertNotNull(typeInfo, "getTypeInfo() should return non-null ResultSet");

    int typeCount = 0;
    while (typeInfo.next()) {
      typeCount++;
      String typeName = typeInfo.getString("TYPE_NAME");
      int dataType = typeInfo.getInt("DATA_TYPE");
      assertNotNull(typeName, "TYPE_NAME should not be null");
      assertFalse(typeName.isEmpty(), "TYPE_NAME should not be empty");
    }
    assertTrue(typeCount > 0, "Should have at least one type in the type catalog");

    typeInfo.close();
  }

  // --- ParameterMetaData tests ---

  @Test
  void testParameterMetaData_GetParameterCount() throws SQLException {
    PreparedStatement pstmt = connection.prepareStatement("SELECT ? AS p1, ? AS p2, ? AS p3");
    ParameterMetaData pmd = pstmt.getParameterMetaData();
    assertNotNull(pmd, "ParameterMetaData should not be null");
    assertEquals(3, pmd.getParameterCount(), "Should detect 3 parameter markers");

    pstmt.close();
  }

  @Test
  void testParameterMetaData_NoParameters() throws SQLException {
    PreparedStatement pstmt = connection.prepareStatement("SELECT 1 AS num");
    ParameterMetaData pmd = pstmt.getParameterMetaData();
    assertNotNull(pmd, "ParameterMetaData should not be null");
    assertEquals(0, pmd.getParameterCount(), "Should detect 0 parameter markers");

    pstmt.close();
  }

  @Test
  void testMetadataOperationsWithHyphenatedIdentifiers() throws SQLException {
    assumeTrue(isSqlExecSdkClient(), "This test only runs for SQL Execution API");
    Connection testConnection = connection;
    if (testConnection == null) {
      testConnection = getValidJDBCConnection();
    }
    DatabaseMetaData metaData = testConnection.getMetaData();

    String existingCatalog = "main";
    String schemaWithHyphens = "test-schema-hyphen";
    String tableWithHyphens = "test-table-hyphen";

    try {
      executeSQL(
          testConnection,
          "CREATE SCHEMA IF NOT EXISTS `" + existingCatalog + "`.`" + schemaWithHyphens + "`");

      executeSQL(
          testConnection,
          "CREATE TABLE IF NOT EXISTS `"
              + existingCatalog
              + "`.`"
              + schemaWithHyphens
              + "`.`"
              + tableWithHyphens
              + "` (id INT, name STRING)");

      try (ResultSet tables = metaData.getTables(existingCatalog, schemaWithHyphens, "%", null)) {
        assertTrue(tables.next(), "Should retrieve tables from schema with hyphens");
        String tableName = tables.getString("TABLE_NAME");
        assertNotNull(tableName, "Table name should not be null");
      }

      try (ResultSet columns =
          metaData.getColumns(existingCatalog, schemaWithHyphens, tableWithHyphens, null)) {
        assertTrue(columns.next(), "Should retrieve columns from table with hyphens");
        String columnName = columns.getString("COLUMN_NAME");
        assertNotNull(columnName, "Column name should not be null");
      }

    } finally {
      executeSQL(
          testConnection,
          "DROP SCHEMA IF EXISTS `" + existingCatalog + "`.`" + schemaWithHyphens + "` CASCADE");
    }
  }
}
