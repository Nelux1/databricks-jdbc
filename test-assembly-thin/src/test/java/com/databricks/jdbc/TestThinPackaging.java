package com.databricks.jdbc;

import com.databricks.jdbc.api.impl.arrow.ArrowBufferAllocator;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Test artifacts are packaged properly. */
public class TestThinPackaging {
  @Test
  public void testThinPackaging() {
    // Test that the "arrow" package is relocated.
    com.databricks.internal.apache.arrow.memory.BufferAllocator bufferAllocator =
        ArrowBufferAllocator.getBufferAllocator();
    System.out.println(bufferAllocator.toString());

    // TODO - add more of these classes to test.
  }

  @Test
  public void executeLargeQuery() throws SQLException {
    try (Connection connection =
        connect(
            Map.of(
                DatabricksJdbcUrlParams.ENABLE_ARROW.getParamName(), "1",
                DatabricksJdbcUrlParams.USE_THRIFT_CLIENT.getParamName(), "0"))) {
      try (Statement statement = connection.createStatement()) {
        final int limit = 1_000_000;
        final String sql = "SELECT * FROM samples.tpch.lineitem LIMIT " + limit;
        ResultSet result = statement.executeQuery(sql);
        int totalRows = 0;
        while (result.next()) {
          if (totalRows % 100_000 == 0) {
            System.out.println("Processed " + totalRows + " rows");
          }
          totalRows++;
        }

        System.out.println(totalRows + " rows processed");
      }
    }
  }

  private Connection connect(Map<String, Object> urlParams) throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", getDatabricksUser());
    props.setProperty("password", getDatabricksDogfoodToken());
    for (Map.Entry<String, Object> entry : urlParams.entrySet()) {
      props.setProperty(entry.getKey(), entry.getValue().toString());
    }

    String url = getDogfoodJDBCUrl();

    return new com.databricks.client.jdbc.Driver().connect(url, props);
  }

  private String getDogfoodJDBCUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksDogfoodHost();
    String httpPath = getDatabricksDogfoodHTTPPath();

    return String.format(template, host, httpPath);
  }

  private String getDatabricksDogfoodHTTPPath() {
    return System.getenv("DATABRICKS_DOGFOOD_HTTP_PATH");
  }

  private String getDatabricksDogfoodHost() {
    return System.getenv("DATABRICKS_DOGFOOD_HOST");
  }

  private String getDatabricksUser() {
    return Optional.ofNullable(System.getenv("DATABRICKS_USER")).orElse("token");
  }

  private String getDatabricksDogfoodToken() {
    return System.getenv("DATABRICKS_DOGFOOD_TOKEN");
  }
}
