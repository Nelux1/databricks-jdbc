package com.jayant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.jayant.testparams.ConnectionTestParams;
import com.jayant.testparams.DatabaseMetaDataTestParams;
import com.jayant.testparams.ResultSetMetaDataTestParams;
import com.jayant.testparams.ResultSetTestParams;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JDBCDriverComparisonTest {
  private static final String COMPARATOR_MODE =
      System.getProperty("COMPARATOR_MODE", "thrift-vs-sea");

  private static final String OLD_DRIVER_JDBC_URL =
      "jdbc:databricks://adb-6436897454825492.12.azuredatabricks.net:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/2f03dd43e35e2aa0;UID=token;";
  private static final String OSS_DRIVER_JDBC_URL =
      "jdbc:databricks://adb-6436897454825492.12.azuredatabricks.net:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/2f03dd43e35e2aa0;useThriftClient=1";
  private static final String OSS_SEA_DRIVER_JDBC_URL =
      "jdbc:databricks://adb-6436897454825492.12.azuredatabricks.net:443/default;ssl=1;authMech=3;httpPath=/sql/1.0/warehouses/2f03dd43e35e2aa0;useThriftClient=0";
  private static Connection oldDriverConnection;
  private static Connection ossThriftConnection;
  private static Connection ossSeaConnection;
  private static Path tempDir;
  private static TestReporter reporter;
  // ResultSets for Old vs SEA comparison
  private static ResultSet oldDriverResultSet;
  private static ResultSet ossSeaResultSet1;
  // ResultSets for Thrift vs SEA comparison
  private static ResultSet ossThriftResultSet;
  private static ResultSet ossSeaResultSet2;

  @BeforeAll
  static void setup() throws Exception {
    // Create temporary directory for extracted JARs
    tempDir = Files.createTempDirectory("jdbc-drivers");

    // Initialize connections
    String pwd = System.getenv("DATABRICKS_COMPARATOR_TOKEN");

    Properties props = new Properties();

    if (COMPARATOR_MODE.equals("simba-vs-sea") || COMPARATOR_MODE.equals("all")) {
      // Extract and load old driver (2.7.6)
      URL oldDriverJarUrl = extractJarToTemp("databricks-jdbc-2.7.6.jar", tempDir);
      if (oldDriverJarUrl == null) {
        throw new RuntimeException("Unable to find JDBC driver JARs in the classpath");
      }
      URLClassLoader oldDriverClassLoader =
          new CustomClassLoader(
              new URL[] {oldDriverJarUrl}, JDBCDriverComparisonTest.class.getClassLoader());
      Class<?> oldDriverClass =
          Class.forName("com.databricks.client.jdbc.Driver", true, oldDriverClassLoader);
      Driver oldDriver = (Driver) oldDriverClass.getDeclaredConstructor().newInstance();
      oldDriverConnection = oldDriver.connect(OLD_DRIVER_JDBC_URL + "PWD=" + pwd, props);
    }

    // OSS driver with Thrift
    ossThriftConnection = DriverManager.getConnection(OSS_DRIVER_JDBC_URL, "token", pwd);

    // OSS driver with SEA
    ossSeaConnection = DriverManager.getConnection(OSS_SEA_DRIVER_JDBC_URL, "token", pwd);

    String timestamp = Instant.now().toString().replaceAll("[:.]+", "-");
    reporter = new TestReporter(Path.of("jdbc-comparison-report-" + timestamp + ".txt"));
    if (COMPARATOR_MODE.equals("simba-vs-sea") || COMPARATOR_MODE.equals("all")) {
      reporter.addConnectionUrl("Old Driver (2.7.6)", OLD_DRIVER_JDBC_URL);
    }
    if (COMPARATOR_MODE.equals("thrift-vs-sea") || COMPARATOR_MODE.equals("all")) {
      reporter.addConnectionUrl("OSS Thrift", OSS_DRIVER_JDBC_URL);
    }
    reporter.addConnectionUrl("OSS SEA", OSS_SEA_DRIVER_JDBC_URL);

    String queryResultSetTypesTable = "select * from samples.tpch.customer limit 100";
    // Create separate ResultSets for each comparison pair to avoid reuse issues
    if (COMPARATOR_MODE.equals("simba-vs-sea") || COMPARATOR_MODE.equals("all")) {
      oldDriverResultSet =
          oldDriverConnection.createStatement().executeQuery(queryResultSetTypesTable);
      ossSeaResultSet1 = ossSeaConnection.createStatement().executeQuery(queryResultSetTypesTable);
    }

    ossThriftResultSet =
        ossThriftConnection.createStatement().executeQuery(queryResultSetTypesTable);
    ossSeaResultSet2 = ossSeaConnection.createStatement().executeQuery(queryResultSetTypesTable);
  }

  @AfterAll
  static void teardown() throws Exception {
    if (oldDriverConnection != null) oldDriverConnection.close();
    if (ossThriftConnection != null) ossThriftConnection.close();
    if (ossSeaConnection != null) ossSeaConnection.close();
    // Clean up temp directory
    if (tempDir != null) {
      Files.walk(tempDir)
          .sorted((p1, p2) -> -p1.compareTo(p2))
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              });
    }
    reporter.generateReport();
  }

  /**
   * Helper method for tests that need connections. Prepends connection pair info (name, conn1,
   * conn2) to original arguments.
   */
  private static Stream<Arguments> withConnectionPairs(Stream<Arguments> baseProvider) {
    List<Arguments> base = baseProvider.collect(Collectors.toList());
    List<Arguments> combined = new ArrayList<>();

    // Define connection pairs based on COMPARATOR_MODE
    List<Object[]> pairList = new ArrayList<>();
    if (COMPARATOR_MODE.equals("simba-vs-sea") || COMPARATOR_MODE.equals("all")) {
      pairList.add(new Object[] {"Old(2.7.6) vs OSS-SEA", oldDriverConnection, ossSeaConnection});
    }
    if (COMPARATOR_MODE.equals("thrift-vs-sea") || COMPARATOR_MODE.equals("all")) {
      pairList.add(new Object[] {"OSS-Thrift vs OSS-SEA", ossThriftConnection, ossSeaConnection});
    }
    Object[][] connectionPairs = pairList.toArray(new Object[0][]);

    // Combine each pair with each base argument
    for (Object[] pair : connectionPairs) {
      for (Arguments arg : base) {
        Object[] originalArgs = arg.get();
        // Create new array: [name, conn1, conn2, ...originalArgs]
        Object[] newArgs = new Object[3 + originalArgs.length];
        newArgs[0] = pair[0]; // comparison name
        newArgs[1] = pair[1]; // connection 1
        newArgs[2] = pair[2]; // connection 2
        System.arraycopy(originalArgs, 0, newArgs, 3, originalArgs.length);
        combined.add(Arguments.of(newArgs));
      }
    }

    return combined.stream();
  }

  /**
   * Helper method for tests that need ResultSets. Prepends ResultSet pair info (name, rs1, rs2) to
   * original arguments.
   */
  private static Stream<Arguments> withResultSetPairs(Stream<Arguments> baseProvider) {
    List<Arguments> base = baseProvider.collect(Collectors.toList());
    List<Arguments> combined = new ArrayList<>();

    // Define ResultSet pairs based on COMPARATOR_MODE - each with separate instances to avoid reuse
    List<Object[]> pairList = new ArrayList<>();
    if (COMPARATOR_MODE.equals("simba-vs-sea") || COMPARATOR_MODE.equals("all")) {
      pairList.add(new Object[] {"Old(2.7.6) vs OSS-SEA", oldDriverResultSet, ossSeaResultSet1});
    }
    if (COMPARATOR_MODE.equals("thrift-vs-sea") || COMPARATOR_MODE.equals("all")) {
      pairList.add(new Object[] {"OSS-Thrift vs OSS-SEA", ossThriftResultSet, ossSeaResultSet2});
    }
    Object[][] resultSetPairs = pairList.toArray(new Object[0][]);

    // Combine each pair with each base argument
    for (Object[] pair : resultSetPairs) {
      for (Arguments arg : base) {
        Object[] originalArgs = arg.get();
        // Create new array: [name, rs1, rs2, ...originalArgs]
        Object[] newArgs = new Object[3 + originalArgs.length];
        newArgs[0] = pair[0]; // comparison name
        newArgs[1] = pair[1]; // resultSet 1
        newArgs[2] = pair[2]; // resultSet 2
        System.arraycopy(originalArgs, 0, newArgs, 3, originalArgs.length);
        combined.add(Arguments.of(newArgs));
      }
    }

    return combined.stream();
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("provideSQLQueries")
  @DisplayName("Compare SQL Query Results")
  void compareSQLQueryResults(
      String comparisonName, Connection conn1, Connection conn2, String query, String description) {
    assertDoesNotThrow(
        () -> {
          ResultSet result1 = conn1.createStatement().executeQuery(query);
          ResultSet result2 = conn2.createStatement().executeQuery(query);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "sql [" + comparisonName + "]", query, new String[] {}, result1, result2);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println(
                "[" + comparisonName + "] Differences found in query results for: " + description);
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("provideMetadataMethods")
  @DisplayName("Compare Metadata API Results")
  void compareMetadataResults(
      String comparisonName, Connection conn1, Connection conn2, String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          System.out.printf(
              "[%s] [%s] Running: %s(%s)%n",
              java.time.Instant.now(), comparisonName, methodName, getStringForArgs(args));

          DatabaseMetaData metadata1 = conn1.getMetaData();
          DatabaseMetaData metadata2 = conn2.getMetaData();

          Object result1 = ReflectionUtils.executeMethod(metadata1, methodName, args);
          Object result2 = ReflectionUtils.executeMethod(metadata2, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "DatabaseMetaData [" + comparisonName + "]", methodName, args, result1, result2);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println(
                "["
                    + comparisonName
                    + "] Differences found in metadata results for method: "
                    + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("provideResultSetMethods")
  @DisplayName("Compare ResultSet API Results")
  void compareResultSetResults(
      String comparisonName, ResultSet rs1, ResultSet rs2, String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          Object result1 = ReflectionUtils.executeMethod(rs1, methodName, args);
          Object result2 = ReflectionUtils.executeMethod(rs2, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "ResultSet [" + comparisonName + "]", methodName, args, result1, result2);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println(
                "["
                    + comparisonName
                    + "] Differences found in ResultSet results for method: "
                    + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("provideResultSetMetaDataMethods")
  @DisplayName("Compare ResultSetMetaData API Results")
  void compareResultSetMetaDataResults(
      String comparisonName, ResultSet rs1, ResultSet rs2, String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          ResultSetMetaData metadata1 = rs1.getMetaData();
          ResultSetMetaData metadata2 = rs2.getMetaData();
          Object result1 = ReflectionUtils.executeMethod(metadata1, methodName, args);
          Object result2 = ReflectionUtils.executeMethod(metadata2, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "ResultSetMetaData [" + comparisonName + "]", methodName, args, result1, result2);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println(
                "["
                    + comparisonName
                    + "] Differences found in ResultSetMetaData results for method: "
                    + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  @ParameterizedTest(autoCloseArguments = false)
  @MethodSource("provideConnectionMethods")
  @DisplayName("Compare Connection API Results")
  void compareConnectionResults(
      String comparisonName, Connection conn1, Connection conn2, String methodName, Object[] args) {
    assertDoesNotThrow(
        () -> {
          Object result1 = ReflectionUtils.executeMethod(conn1, methodName, args);
          Object result2 = ReflectionUtils.executeMethod(conn2, methodName, args);

          ComparisonResult result =
              ResultSetComparator.compare(
                  "Connection [" + comparisonName + "]", methodName, args, result1, result2);
          reporter.addResult(result);

          if (result.hasDifferences()) {
            System.err.println(
                "["
                    + comparisonName
                    + "] Differences found in Connection results for method: "
                    + methodName);
            System.err.println("Args: " + getStringForArgs(args));
            System.err.println(result);
          }
        });
  }

  private static Stream<Arguments> provideSQLQueries() {
    Stream<Arguments> base =
        Stream.of(
            Arguments.of("SELECT * FROM samples.tpcds_sf1.catalog_sales limit 5", "TPC-DS query"));
    return withConnectionPairs(base);
  }

  private static Stream<Arguments> provideMetadataMethods() {
    DatabaseMetaDataTestParams params = new DatabaseMetaDataTestParams();
    Stream<Arguments> base = ReflectionUtils.provideMethodsForClass(DatabaseMetaData.class, params);
    return withConnectionPairs(base);
  }

  private static Stream<Arguments> provideResultSetMethods() {
    ResultSetTestParams params = new ResultSetTestParams();
    Stream<Arguments> base = ReflectionUtils.provideMethodsForClass(ResultSet.class, params);
    return withResultSetPairs(base);
  }

  private static Stream<Arguments> provideResultSetMetaDataMethods() {
    ResultSetMetaDataTestParams params = new ResultSetMetaDataTestParams();
    Stream<Arguments> base =
        ReflectionUtils.provideMethodsForClass(ResultSetMetaData.class, params);
    return withResultSetPairs(base);
  }

  private static Stream<Arguments> provideConnectionMethods() {
    ConnectionTestParams params = new ConnectionTestParams();
    Stream<Arguments> base = ReflectionUtils.provideMethodsForClass(Connection.class, params);
    return withConnectionPairs(base);
  }

  private static URL extractJarToTemp(String jarName, Path tempDir) {
    try {
      try (InputStream in = JDBCDriverComparisonTest.class.getResourceAsStream("/" + jarName)) {
        if (in == null) {
          throw new RuntimeException("Could not find " + jarName + " in resources");
        }
        Path targetPath = tempDir.resolve(jarName);
        Files.copy(in, targetPath, StandardCopyOption.REPLACE_EXISTING);
        return targetPath.toUri().toURL();
      }
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private static String getStringForArgs(Object[] args) {
    return Arrays.stream(args)
        .map(
            o -> {
              if (o == null) return "null";
              if (o instanceof Object[]) return Arrays.toString((Object[]) o);
              return o.toString();
            })
        .collect(Collectors.joining(", "));
  }
}
