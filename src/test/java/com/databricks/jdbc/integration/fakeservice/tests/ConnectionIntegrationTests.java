package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Integration tests for connection to Databricks service. */
public class ConnectionIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testSuccessfulConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assert ((conn != null) && !conn.isClosed());

    conn.close();
  }

  @Test
  void testIncorrectCredentialsForPAT() {
    Properties extraProps = new Properties();
    extraProps.put(DatabricksJdbcUrlParams.UID.getParamName(), getDatabricksUser());
    extraProps.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), "bad_token_1");
    String url = getFakeServiceJDBCUrl();
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, createConnectionProperties(extraProps)));

    assert e.getMessage()
        .contains("Connection failure while using the OSS Databricks JDBC driver.");
  }

  @Test
  void testIncorrectCredentialsForOAuth() {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=11;AuthFlow=0;httpPath=%s";
    String url =
        String.format(
            template,
            getFakeServiceHost(),
            FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.HTTP_PATH.getParamName()));

    Properties extraProps = new Properties();
    extraProps.put(DatabricksJdbcUrlParams.UID.getParamName(), getDatabricksUser());
    extraProps.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), "bad_token_2");
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> DriverManager.getConnection(url, createConnectionProperties(extraProps)));

    assert e.getMessage()
        .contains("Connection failure while using the OSS Databricks JDBC driver.");
  }

  @Test
  void testPATinOAuthTokenPassThrough() throws Exception {
    // SSL is disabled as embedded web server of fake service uses HTTP protocol.
    // Note that in RECORD mode, the web server interacts with production services over HTTPS.
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=11;AuthFlow=0;httpPath=%s;";
    String url =
        String.format(
            template,
            getFakeServiceHost(),
            FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.HTTP_PATH.getParamName()));
    Properties extraProps = new Properties();
    extraProps.put(DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(), getDatabricksToken());
    Connection conn = DriverManager.getConnection(url, createConnectionProperties(extraProps));
    assert ((conn != null) && !conn.isClosed());

    conn.close();
  }

  // --- Connection properties and management tests ---

  @Test
  void testIsClosed_NewConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();
    assertFalse(conn.isClosed(), "Newly created connection should not be closed");

    conn.close();
    assertTrue(conn.isClosed(), "Connection should be closed after close()");
  }

  @Test
  void testIsValid_ActiveConnection() throws SQLException {
    Connection conn = getValidJDBCConnection();

    // isValid with a positive timeout should return true for an active connection
    assertTrue(conn.isValid(5), "Active connection should be valid");

    conn.close();
    assertFalse(conn.isValid(5), "Closed connection should not be valid");
  }

  @Test
  void testGetCatalog_ReturnsNonNull() throws SQLException {
    Connection conn = getValidJDBCConnection();

    String catalog = conn.getCatalog();
    assertNotNull(catalog, "getCatalog() should return non-null for active connection");
    assertFalse(catalog.isEmpty(), "getCatalog() should return non-empty string");

    conn.close();
  }

  // --- Transaction and connection attribute tests ---

  @Test
  void testAutoCommit_DefaultIsTrue() throws SQLException {
    Connection conn = getValidJDBCConnection();

    assertTrue(conn.getAutoCommit(), "Default autoCommit should be true");

    conn.close();
  }

  @Test
  void testGetSchema_ReturnsNonNull() throws SQLException {
    Connection conn = getValidJDBCConnection();

    String schema = conn.getSchema();
    assertNotNull(schema, "getSchema() should return non-null for active connection");
    assertFalse(schema.isEmpty(), "getSchema() should return non-empty string");

    conn.close();
  }

  @Test
  void testIsReadOnly_Default() throws SQLException {
    Connection conn = getValidJDBCConnection();

    boolean readOnly = conn.isReadOnly();
    assertFalse(readOnly, "Default connection should not be read-only");

    conn.close();
  }

  @Test
  void testGetMetaData_ReturnsNonNull() throws SQLException {
    Connection conn = getValidJDBCConnection();

    DatabaseMetaData metaData = conn.getMetaData();
    assertNotNull(metaData, "getMetaData() should return non-null");
    assertNotNull(metaData.getDriverName(), "Driver name should not be null");
    assertFalse(metaData.getDriverName().isEmpty(), "Driver name should not be empty");

    conn.close();
  }

  @Test
  void testGetWarnings_AndClearWarnings() throws SQLException {
    Connection conn = getValidJDBCConnection();

    // New connection may or may not have warnings, but getWarnings() should not throw
    SQLWarning warnings = conn.getWarnings();
    // clearWarnings should not throw
    conn.clearWarnings();
    assertNull(conn.getWarnings(), "Warnings should be null after clearWarnings()");

    conn.close();
  }

  @Test
  void testGetClientInfo_ReturnsProperties() throws SQLException {
    Connection conn = getValidJDBCConnection();

    Properties clientInfo = conn.getClientInfo();
    assertNotNull(clientInfo, "getClientInfo() should return non-null Properties");

    conn.close();
  }

  @Test
  void testGetTransactionIsolation() throws SQLException {
    Connection conn = getValidJDBCConnection();

    int isolation = conn.getTransactionIsolation();
    assertTrue(
        isolation == Connection.TRANSACTION_NONE
            || isolation == Connection.TRANSACTION_READ_UNCOMMITTED
            || isolation == Connection.TRANSACTION_READ_COMMITTED
            || isolation == Connection.TRANSACTION_REPEATABLE_READ
            || isolation == Connection.TRANSACTION_SERIALIZABLE,
        "Transaction isolation should be a valid JDBC constant");

    conn.close();
  }

  private Properties createConnectionProperties(Properties extraProps) {
    Properties connProps = new Properties();
    connProps.putAll(extraProps);
    connProps.put(
        DatabricksJdbcUrlParams.CONN_CATALOG.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_CATALOG.getParamName()));
    connProps.put(
        DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName(),
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA.getParamName()));
    connProps.put(
        DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName(),
        FakeServiceConfigLoader.shouldUseThriftClient());

    return connProps;
  }
}
