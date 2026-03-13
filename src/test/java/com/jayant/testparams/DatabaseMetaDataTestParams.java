package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;
import static com.jayant.testparams.ParamUtils.registerCombinations;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

public class DatabaseMetaDataTestParams implements TestParams {

  // ---------------------------------------------------------------------------
  // Argument variant lists — functionToArgsMaps to the test plan's argument variant table.
  //
  // Workspace: peco testing workspace
  //   catalog  : "comparator-tests" (hyphen) / "comparator_tests" (underscore)
  //   schema   : "oss-jdbc-tests" (hyphen) / "oss_jdbc_tests" (underscore)
  //   table    : "test-result-set-types" (hyphen) / "test_result_set_types" (underscore)
  //   column   : "varchar-column" (hyphen) / "varchar_column" (underscore)
  //   function : "area-calc" (hyphen) / "area_calc" (underscore)
  //
  // Hyphen variants are used as the base for exact match.
  // Underscore variants test unescaped _ (wildcard) and escaped \_ (literal) behavior.
  // ---------------------------------------------------------------------------

  static final List<Object> CATALOG_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "comparator-tests", // exact (hyphen)
          "COMPARATOR-TESTS", // uppercase
          "comparator_tests", // unescaped _ wildcard
          "comp%", // prefix pattern
          "comparator\\_tests", // escaped _ literal
          "nonexistent");

  static final List<Object> SCHEMA_PATTERN_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "oss-jdbc-tests", // exact (hyphen)
          "OSS-JDBC-TESTS", // uppercase
          "oss_jdbc_tests", // unescaped _ wildcard
          "oss%", // prefix pattern
          "oss\\_jdbc\\_tests", // escaped _ literal
          "nonexistent");

  static final List<Object> TABLE_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "test-result-set-types", // exact (hyphen)
          "TEST-RESULT-SET-TYPES", // uppercase
          "test_result_set_types", // unescaped _ wildcard
          "test%", // prefix pattern
          "test\\_result\\_set\\_types", // escaped _ literal
          "nonexistent");

  static final List<Object> COLUMN_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "varchar-column", // exact (hyphen)
          "VARCHAR-COLUMN", // uppercase
          "varchar_column", // unescaped _ wildcard
          "varchar%", // prefix pattern
          "varchar\\_column", // escaped _ literal
          "nonexistent");

  static final List<Object> FUNCTION_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "area-calc", // exact (hyphen)
          "AREA-CALC", // uppercase
          "area_calc", // unescaped _ wildcard
          "area%", // prefix pattern
          "area\\_calc", // escaped _ literal
          "nonexistent");

  // 7 type-filter variants for getTables — independent dimension
  static final List<Object> TABLE_TYPE_VARIANTS =
      Arrays.asList(
          null,
          new String[] {"TABLE"},
          new String[] {"VIEW"},
          new String[] {"TABLE", "VIEW"},
          new String[] {},
          new String[] {"NONEXISTENT_TYPE"},
          new String[] {"table"});

  // ---------------------------------------------------------------------------
  // Key method variant lists — patterns not supported, exact-match + null only
  // ---------------------------------------------------------------------------

  // Exact-match catalog/schema for key methods (no patterns)
  static final List<Object> KEYS_CATALOG_VARIANTS = Arrays.asList(null, "comparator_tests");

  static final List<Object> KEYS_SCHEMA_VARIANTS = Arrays.asList(null, "oss_jdbc_tests");

  // For getPrimaryKeys: tables with PK + no-PK negative case
  static final List<Object> PK_TABLE_VARIANTS =
      Arrays.asList(
          null,
          "test_result_set_types", // PK(id, varchar_column)
          "no_constraints", // no PK — negative case
          "fk_child", // PK(child_id)
          "fk_parent" // PK(parent_id)
          );

  // For getExportedKeys: parent tables
  static final List<Object> PARENT_TABLE_VARIANTS = Arrays.asList(null, "fk_parent");

  // For getImportedKeys: child tables + no-FK negative case
  static final List<Object> FK_TABLE_VARIANTS =
      Arrays.asList(
          null,
          "fk_child", // has FK → fk_parent
          "no_constraints", // no FK — negative case
          "test_result_set_types" // no FK
          );

  // ---------------------------------------------------------------------------
  // Test registration
  // ---------------------------------------------------------------------------

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();
    registerResultSetMethods(functionToArgsMap);
    registerScalarMethods(functionToArgsMap);
    return functionToArgsMap;
  }

  private void registerResultSetMethods(
      Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap) {
    // 1.1 getCatalogs() — 0-arg, auto-tested by reflection. Nothing to register.

    // 1.2 getSchemas(catalog, schemaPattern)
    registerCombinations(
        functionToArgsMap, "getSchemas", CATALOG_VARIANTS, SCHEMA_PATTERN_VARIANTS);

    // 1.3 getTables(catalog, schemaPattern, tableNamePattern, types[])
    registerCombinations(
        functionToArgsMap,
        "getTables",
        CATALOG_VARIANTS,
        SCHEMA_PATTERN_VARIANTS,
        TABLE_VARIANTS,
        TABLE_TYPE_VARIANTS);

    // 1.4 getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)
    registerCombinations(
        functionToArgsMap,
        "getColumns",
        CATALOG_VARIANTS,
        SCHEMA_PATTERN_VARIANTS,
        TABLE_VARIANTS,
        COLUMN_VARIANTS);

    // 1.5 getTableTypes — 0-arg, auto-tested by reflection
    // 1.6 getTypeInfo — 0-arg, auto-tested by reflection

    // 1.7 getPrimaryKeys — exact-match only (patterns not supported)
    registerCombinations(
        functionToArgsMap,
        "getPrimaryKeys",
        KEYS_CATALOG_VARIANTS,
        KEYS_SCHEMA_VARIANTS,
        PK_TABLE_VARIANTS);

    // 1.8 getImportedKeys — exact-match only (patterns not supported)
    registerCombinations(
        functionToArgsMap,
        "getImportedKeys",
        KEYS_CATALOG_VARIANTS,
        KEYS_SCHEMA_VARIANTS,
        FK_TABLE_VARIANTS);

    // 1.9 getExportedKeys — exact-match only (both drivers return empty)
    registerCombinations(
        functionToArgsMap,
        "getExportedKeys",
        KEYS_CATALOG_VARIANTS,
        KEYS_SCHEMA_VARIANTS,
        PARENT_TABLE_VARIANTS);
    // 1.10 getCrossReference — curated cases (6 args not independent, patterns not supported)
    Object[][] crossRefCases = {
      {null, null, null, null, null, null},
      {null, null, null, "comparator_tests", "oss_jdbc_tests", "fk_child"},
      {"comparator_tests", "oss_jdbc_tests", "fk_parent", null, null, null},
      {
        "comparator_tests",
        "oss_jdbc_tests",
        "fk_parent",
        "comparator_tests",
        "oss_jdbc_tests",
        "fk_child"
      },
      {
        "comparator_tests",
        "oss_jdbc_tests",
        "fk_parent",
        "comparator_tests",
        "oss-jdbc-tests",
        "fk_child_cross_schema"
      },
      {
        "comparator_tests",
        "oss_jdbc_tests",
        "fk_parent",
        "comparator-tests",
        "oss_jdbc_tests",
        "fk_child_cross_catalog"
      },
      {
        "comparator_tests",
        "oss_jdbc_tests",
        "no_constraints",
        "comparator_tests",
        "oss_jdbc_tests",
        "no_constraints"
      },
      {
        "comparator_tests",
        "oss_jdbc_tests",
        "fk_child",
        "comparator_tests",
        "oss_jdbc_tests",
        "fk_parent"
      },
      {
        "comparator_tests",
        "oss_jdbc_tests",
        "nonexistent",
        "comparator_tests",
        "oss_jdbc_tests",
        "nonexistent"
      },
    };
    for (Object[] args : crossRefCases) {
      putInMapForKey(functionToArgsMap, Map.entry("getCrossReference", 6), args);
    }
    // 1.11 getFunctions(catalog, schemaPattern, functionNamePattern)
    registerCombinations(
        functionToArgsMap,
        "getFunctions",
        CATALOG_VARIANTS,
        SCHEMA_PATTERN_VARIANTS,
        FUNCTION_VARIANTS);
    // 1.12 Stub methods — all return empty ResultSet; single representative arg set each
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getFunctionColumns", 4),
        new String[] {"comparator_tests", "oss_jdbc_tests", "area_calc", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getProcedures", 3),
        new String[] {"comparator_tests", "oss_jdbc_tests", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getProcedureColumns", 4),
        new String[] {"comparator_tests", "oss_jdbc_tests", "%", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getColumnPrivileges", 4),
        new String[] {"comparator_tests", "oss_jdbc_tests", "test_result_set_types", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getTablePrivileges", 3),
        new String[] {"comparator_tests", "oss_jdbc_tests", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getIndexInfo", 5),
        new Object[] {"comparator_tests", "oss_jdbc_tests", "test_result_set_types", true, false});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getVersionColumns", 3),
        new String[] {"comparator_tests", "oss_jdbc_tests", "test_result_set_types"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getUDTs", 4),
        new String[] {"comparator_tests", "oss_jdbc_tests", "%", null});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getSuperTypes", 3),
        new String[] {"comparator_tests", "oss_jdbc_tests", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getSuperTables", 3),
        new String[] {"comparator_tests", "oss_jdbc_tests", "test_result_set_types"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getAttributes", 4),
        new String[] {"comparator_tests", "oss_jdbc_tests", "%", "%"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getPseudoColumns", 4),
        new String[] {"comparator_tests", "oss_jdbc_tests", "test_result_set_types", "%"});
    // getBestRowIdentifier — 3 scope values
    for (Integer scope : getAllBestRowIdentifierScopes()) {
      putInMapForKey(
          functionToArgsMap,
          Map.entry("getBestRowIdentifier", 5),
          new Object[] {
            "comparator_tests", "oss_jdbc_tests", "test_result_set_types", scope, true
          });
    }
  }

  private void registerScalarMethods(
      Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap) {
    for (Integer type : getResultSetTypes()) {
      putInMapForKey(
          functionToArgsMap, Map.entry("supportsResultSetType", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap,
          Map.entry("supportsResultSetConcurrency", 2),
          new Integer[] {type, ResultSet.CONCUR_READ_ONLY});
      putInMapForKey(
          functionToArgsMap,
          Map.entry("supportsResultSetConcurrency", 2),
          new Integer[] {type, ResultSet.CONCUR_UPDATABLE});
      putInMapForKey(functionToArgsMap, Map.entry("ownUpdatesAreVisible", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("ownDeletesAreVisible", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("ownInsertsAreVisible", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap, Map.entry("othersUpdatesAreVisible", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap, Map.entry("othersDeletesAreVisible", 1), new Integer[] {type});
      putInMapForKey(
          functionToArgsMap, Map.entry("othersInsertsAreVisible", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("updatesAreDetected", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("deletesAreDetected", 1), new Integer[] {type});
      putInMapForKey(functionToArgsMap, Map.entry("insertsAreDetected", 1), new Integer[] {type});
    }
    for (Integer i : getAllTransactionIsolationLevels()) {
      putInMapForKey(
          functionToArgsMap, Map.entry("supportsTransactionIsolationLevel", 1), new Integer[] {i});
    }
    for (Integer i : getResultSetHoldability()) {
      putInMapForKey(
          functionToArgsMap, Map.entry("supportsResultSetHoldability", 1), new Integer[] {i});
    }
    for (Integer fromType : getAllSqlTypes()) {
      for (Integer toType : getAllSqlTypes()) {
        if (fromType == Types.ROWID || toType == Types.ROWID) {
          continue; // ROWID is not supported by Databricks server
        }
        if (fromType == Types.ARRAY || fromType == Types.STRUCT || fromType == Types.OTHER) {
          continue; // Complex types not supported by legacy driver
        }
        if (fromType == Types.BOOLEAN && toType == Types.BOOLEAN) {
          // This is a bug in the legacy driver
          continue;
        }
        putInMapForKey(
            functionToArgsMap, Map.entry("supportsConvert", 2), new Integer[] {fromType, toType});
      }
    }
  }

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = new HashSet<>();
    // don't compare classes
    acceptedKnownDiffs.add(Map.entry("getConnection", 0));

    // don't compare driver version
    acceptedKnownDiffs.add(Map.entry("getDriverVersion", 0));

    // URL passes is different
    acceptedKnownDiffs.add(Map.entry("getURL", 0));

    // Methods that we do not need to test from the Super class
    acceptedKnownDiffs.add(Map.entry("unwrap", 1));
    acceptedKnownDiffs.add(Map.entry("isWrapperFor", 1));
    return acceptedKnownDiffs;
  }

  private static List<Integer> getAllSqlTypes() {
    List<Integer> sqlTypes = new ArrayList<>();

    // Get all fields from the Types class
    Field[] fields = Types.class.getFields();

    for (Field field : fields) {
      if (field.getType().equals(int.class)) { // Only consider fields of type int (SQL types)
        try {
          // Add each constant value to the list
          sqlTypes.add((Integer) field.get(null));
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    }

    return sqlTypes;
  }

  private static List<Integer> getAllTransactionIsolationLevels() {
    return new ArrayList<>(
        Arrays.asList(
            Connection.TRANSACTION_NONE,
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE));
  }

  private static List<Integer> getAllBestRowIdentifierScopes() {
    return new ArrayList<>(
        Arrays.asList(
            DatabaseMetaData.bestRowTemporary,
            DatabaseMetaData.bestRowTransaction,
            DatabaseMetaData.bestRowSession));
  }

  private static List<Integer> getResultSetTypes() {
    return new ArrayList<>(
        Arrays.asList(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.TYPE_SCROLL_SENSITIVE));
  }

  private static List<Integer> getResultSetHoldability() {
    return new ArrayList<>(
        Arrays.asList(ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT));
  }
}
