package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DatabricksBufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Creates {@link BufferAllocator} instances.
 *
 * <p>First tries to create a {@link RootAllocator} which uses off-heap memory and is faster. If
 * that fails (usually due to JVM reflection restrictions), falls back to {@link
 * DatabricksBufferAllocator} which uses heap memory.
 */
public class ArrowBufferAllocator {
  /** Can a {@code RootAllocator} be created in this JVM instance? */
  private static final boolean canCreateRootAllocator;

  /** Logger instance. */
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowBufferAllocator.class);

  /* Check if the RootAllocator can be instantiated. */
  static {
    RootAllocator rootAllocator = null;
    try {
      rootAllocator = new RootAllocator();
    } catch (Throwable t) {
      String message = t.getMessage();
      if (message == null) {
        message = t.getCause() != null ? t.getCause().getMessage() : "";
      }
      LOGGER.info(
          "Failed to create RootAllocator, will use DatabricksBufferAllocator as fallback: "
              + message);
    }

    canCreateRootAllocator = rootAllocator != null;
    if (rootAllocator != null) {
      try {
        rootAllocator.close();
      } catch (Throwable t) {
        LOGGER.warn("RootAllocator could not be closed: " + t.getMessage());
      }
    }
  }

  /**
   * @return an instance of the {@code BufferAllocator}.
   */
  public static BufferAllocator getBufferAllocator() {
    if (canCreateRootAllocator) {
      return new RootAllocator();
    } else {
      return new DatabricksBufferAllocator();
    }
  }
}
