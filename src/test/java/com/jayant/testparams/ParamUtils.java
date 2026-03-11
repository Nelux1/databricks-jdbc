package com.jayant.testparams;

import java.sql.ResultSet;
import java.util.*;

public class ParamUtils {
  static void putInMapForKey(
      Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap,
      Map.Entry<String, Integer> key,
      Object[] value) {
    functionToArgsMap.putIfAbsent(key, new HashSet<>());
    functionToArgsMap.get(key).add(value);
  }

  /**
   * Registers all cartesian product combinations of the given per-argument variant lists into the
   * map under (methodName, arity).
   */
  static void registerCombinations(
      Map<Map.Entry<String, Integer>, Set<Object[]>> map,
      String methodName,
      List<Object>... argVariants) {
    int arity = argVariants.length;
    for (Object[] combo : cartesianProduct(Arrays.asList(argVariants))) {
      putInMapForKey(map, Map.entry(methodName, arity), combo);
    }
  }

  /** Returns the cartesian product of the given lists as a list of Object arrays. */
  static List<Object[]> cartesianProduct(List<List<Object>> lists) {
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[0]);
    for (List<Object> list : lists) {
      List<Object[]> next = new ArrayList<>();
      for (Object[] prefix : result) {
        for (Object val : list) {
          Object[] combo = Arrays.copyOf(prefix, prefix.length + 1);
          combo[prefix.length] = val;
          next.add(combo);
        }
      }
      result = next;
    }
    return result;
  }

  public static List<Integer> getAllFetchDirection() {
    return Arrays.asList(ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN);
  }

  public static List<Integer> getAllConcurrencyCondition() {
    return Arrays.asList(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE);
  }
}
