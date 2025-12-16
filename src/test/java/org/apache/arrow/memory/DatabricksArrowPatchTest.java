package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the patched allocator works. */
public class DatabricksArrowPatchTest {
  private static final Logger logger = LoggerFactory.getLogger(DatabricksArrowPatchTest.class);

  /** Path to an arrow chunk. */
  private static final Path ARROW_CHUNK_PATH = Path.of("arrow", "chunk_1.arrow");

  /** Path to a LZ4 compressed arrow chunk. */
  private static final Path ARROW_CHUNK_COMPRESSED_PATH = Path.of("arrow", "chunk_1.arrow.lz4");

  /** Acceptable perf hit ratio of the patched Arrow vs native Arrow. */
  private static final double DEFAULT_PERF_HIT_RATIO = 0.30;

  /** System property name to set the perf hit ratio. */
  private static final String PERF_HIT_RATIO_PROPERTY_NAME = "test.arrow.perf.hit.ratio";

  /** Compressed Arrow file suffix. */
  private static final String ARROW_CHUNK_COMPRESSED_FILE_SUFFIX = ".lz4";

  /** Default number of concurrent threads. */
  private static final int DEFAULT_NUM_THREADS = Runtime.getRuntime().availableProcessors();

  /** System property name to set the number of threads */
  private static final String NUM_THREADS_PROPERTY_NAME = "test.arrow.num.threads";

  /** Default iterations per thread. */
  private static final int DEFAULT_CONCURRENT_ITERATIONS_PER_THREAD = 100;

  /** System property name to set the number of iterations per thread. */
  private static final String CONCURRENT_ITERATIONS_PER_THREAD_PROPERTY_NAME =
      "test.arrow.iterations.per.thread";

  /**
   * Test exception is thrown when jvm arg "--add-opens=java.base/java.nio=ALL-UNNAMED" is missing
   * on JVM >= 17.
   */
  @Test
  @Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
  @EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
  public void testArrowThrowsExceptionOnMissingAddOpensJvmArgs() {
    Throwable throwable = null;
    try {
      RootAllocator allocator = new RootAllocator();
      allocator.close(); // Unreachable code.
    } catch (Throwable t) {
      throwable = t;
    }

    assertNotNull(throwable);
    assertTrue(throwable.getCause().getMessage().contains("Failed to initialize MemoryUtil"));
  }

  /**
   * Test patched Arrow buffer allocator works when jvm arg
   * "--add-opens=java.base/java.nio=ALL-UNNAMED" is missing.
   */
  @Test
  @Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
  @EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
  public void testPatchedArrowWorksWithMissingAddOpensJvmArgs() throws IOException {
    for (Path path : Arrays.asList(ARROW_CHUNK_PATH, ARROW_CHUNK_COMPRESSED_PATH)) {
      try (DatabricksBufferAllocator allocator = new DatabricksBufferAllocator()) {
        List<Map<String, Object>> records = parseArrowStream(path, allocator);
        assertFalse(records.isEmpty(), "Some records should be parsed");

        logger.info("Parsed {} records from path {}", records.size(), path);
      }
    }
  }

  /** Parse files concurrently. Test for any memory leaks */
  @Test
  public void testConcurrentExecution() {
    int numThreads =
        System.getProperty(NUM_THREADS_PROPERTY_NAME) == null
            ? DEFAULT_NUM_THREADS
            : Integer.parseInt(System.getProperty(NUM_THREADS_PROPERTY_NAME));
    int iterationsPerThread =
        System.getProperty(CONCURRENT_ITERATIONS_PER_THREAD_PROPERTY_NAME) == null
            ? DEFAULT_CONCURRENT_ITERATIONS_PER_THREAD
            : Integer.parseInt(System.getProperty(CONCURRENT_ITERATIONS_PER_THREAD_PROPERTY_NAME));

    int totalIterations = numThreads * iterationsPerThread;
    logger.info("Num threads {}, Total iterations: {}", numThreads, totalIterations);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      IntStream.range(0, totalIterations)
          .mapToObj(
              i ->
                  executor.submit(
                      () -> {
                        try {
                          parseArrowStream(
                              i % 2 == 0 ? ARROW_CHUNK_PATH : ARROW_CHUNK_COMPRESSED_PATH,
                              new DatabricksBufferAllocator());
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }))
          .forEach(
              future -> {
                try {
                  future.get();
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Test execution speed performance of patched against native Arrow and fail the test of the
   * difference in performance is significant.
   */
  @Test
  public void testPerformance() throws IOException {
    // Disable Arrow debug.
    System.setProperty(ArrowBuf.DEBUG_ALLOCATOR, "false");

    String perfHitRatioProperty = System.getProperty(PERF_HIT_RATIO_PROPERTY_NAME);
    double perfHitRatio =
        perfHitRatioProperty == null
            ? DEFAULT_PERF_HIT_RATIO
            : Double.parseDouble(perfHitRatioProperty);

    logger.info("Perf hit ratio: {}", perfHitRatio);

    testPerformance(ARROW_CHUNK_PATH, perfHitRatio);
    testPerformance(ARROW_CHUNK_COMPRESSED_PATH, perfHitRatio);
  }

  /** Test performance of running the native and patched Arrow. */
  private void testPerformance(Path filePath, double perfHitRatio) throws IOException {
    int warmupIterations = 20;
    int totalIterations = 100;

    List<Long> arrowTimes =
        profileParsing(filePath, totalIterations, warmupIterations, RootAllocator::new);
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    double arrowMean = arrowTimes.stream().mapToLong(Long::longValue).average().getAsDouble();
    logger.info("Arrow parsing times for {} is {}", filePath, arrowTimes);
    logger.info("Arrow mean for {} is {}", filePath, arrowMean);

    List<Long> patchedArrowTimes =
        profileParsing(filePath, totalIterations, warmupIterations, DatabricksBufferAllocator::new);
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    double patchedArrowMean =
        patchedArrowTimes.stream().mapToLong(Long::longValue).average().getAsDouble();
    logger.info("Patched Arrow parsing times for {} is {}", filePath, patchedArrowTimes);
    logger.info("Patched Arrow mean for {} is {}", filePath, patchedArrowMean);

    assertTrue(
        patchedArrowMean < (arrowMean + perfHitRatio * arrowMean),
        "Patched Arrow performance "
            + patchedArrowMean
            + " much lesser than native Arrow "
            + arrowMean);
  }

  /** Profile parsing time for a file. */
  private List<Long> profileParsing(
      Path filePath,
      int totalIterations,
      int warmupIterations,
      Supplier<BufferAllocator> allocatorSupplier) {
    return IntStream.range(0, totalIterations)
        .mapToLong(
            i -> {
              try (BufferAllocator allocator = allocatorSupplier.get()) {
                long start = System.nanoTime();
                try {
                  parseArrowStream(filePath, allocator);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                long end = System.nanoTime();
                return Duration.ofNanos(end - start).toMillis();
              }
            })
        .skip(warmupIterations)
        .boxed()
        .collect(Collectors.toList());
  }

  /** Test that the patched DatabricksArrowBuf parses records correctly. */
  @Test
  public void testPatchedArrowParsing() throws IOException {
    testParsing(ARROW_CHUNK_PATH);
    testParsing(ARROW_CHUNK_COMPRESSED_PATH);
  }

  /**
   * Parse the Arrow stream file at {@code filePath} and compare the records returned by native
   * Arrow and patched Arrow.
   */
  private void testParsing(Path filePath) throws IOException {
    try (RootAllocator rootAllocator = new RootAllocator();
        DatabricksBufferAllocator patchedAllocator = new DatabricksBufferAllocator()) {

      // Parse with Arrow.
      logger.info("Parsing {} with Arrow RootAllocator", filePath);
      List<Map<String, Object>> rootAllocatorRecords = parseArrowStream(filePath, rootAllocator);
      logger.info("RootAllocator records: {}", rootAllocatorRecords.size());

      // Parse with Patched Arrow.
      logger.info("Parsing {} with Arrow patched DatabricksBufferAllocator", filePath);
      List<Map<String, Object>> patchedAllocatorRecords =
          parseArrowStream(filePath, patchedAllocator);
      logger.info("DatabricksBufferAllocator records: {}", patchedAllocatorRecords.size());

      // Assert that records exist and same number of records are parsed.
      assertFalse(rootAllocatorRecords.isEmpty(), "Some records should be parsed");
      assertEquals(
          rootAllocatorRecords.size(),
          patchedAllocatorRecords.size(),
          "Both should parse same number of records");

      // Log a sample record.
      logger.info("Sample record {}", rootAllocatorRecords.get(0));

      // Compare all records parsed using both allocators.
      for (int i = 0; i < patchedAllocatorRecords.size(); i++) {
        Map<String, Object> patchedRecord = patchedAllocatorRecords.get(i);
        Map<String, Object> rootRecord = rootAllocatorRecords.get(i);
        assertEquals(
            rootRecord.keySet(), patchedRecord.keySet(), "Same number of columns should be parsed");

        for (String key : patchedRecord.keySet()) {
          Object patchedColumn = patchedRecord.get(key);
          Object rootColumn = rootRecord.get(key);
          assertEquals(rootColumn, patchedColumn, "Column " + key + " should be same");
        }
      }
    }
  }

  /** Parse the Arrow stream file stored at {@code filePath} and return the records in the file. */
  private List<Map<String, Object>> parseArrowStream(Path filePath, BufferAllocator allocator)
      throws IOException {
    ArrayList<Map<String, Object>> records = new ArrayList<>();

    try (InputStream arrowStream = getStream(filePath);
        ArrowStreamReader reader = new ArrowStreamReader(arrowStream, allocator)) {
      // Iterate over batches.
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        // Transfer all vectors.
        List<ValueVector> valueVectors =
            root.getFieldVectors().stream()
                .map(
                    fieldVector -> {
                      TransferPair transferPair = fieldVector.getTransferPair(allocator);
                      transferPair.transfer();
                      return transferPair.getTo();
                    })
                .collect(Collectors.toList());

        // Parse and populate each record/row in this batch.
        try {
          for (int recordIndex = 0; recordIndex < root.getRowCount(); recordIndex++) {
            HashMap<String, Object> record = new HashMap<>();
            for (ValueVector valueVector : valueVectors) {
              record.put(valueVector.getField().getName(), valueVector.getObject(recordIndex));
            }
            records.add(record);
          }
        } finally {
          // Close all transferred vectors to prevent memory leak
          valueVectors.forEach(ValueVector::close);
        }
      }
    }

    return records;
  }

  /**
   * @return an input stream for the filePath.
   */
  private InputStream getStream(Path filePath) throws IOException {
    InputStream arrowStream = getClass().getClassLoader().getResourceAsStream(filePath.toString());
    assertNotNull(arrowStream, filePath + " not found");
    return filePath.toString().endsWith(ARROW_CHUNK_COMPRESSED_FILE_SUFFIX)
        ? new LZ4FrameInputStream(arrowStream)
        : arrowStream;
  }
}
