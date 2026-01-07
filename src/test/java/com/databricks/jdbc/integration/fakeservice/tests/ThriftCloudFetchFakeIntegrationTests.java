package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksStatement;
import com.databricks.jdbc.api.impl.arrow.ChunkProvider;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.model.core.ExternalLink;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration test to test CloudFetch using StreamingChunkProvider with Thrift client. */
public class ThriftCloudFetchFakeIntegrationTests extends AbstractFakeServiceIntegrationTests {
  /** Connection to the Databricks cluster. */
  private Connection connection;

  /** Table with a lot of rows to generate multiple CloudFetch chunks. */
  private static final String TABLE = "samples.tpch.lineitem";

  private static final Logger LOGGER =
      LogManager.getLogger(ThriftCloudFetchFakeIntegrationTests.class);

  @BeforeEach
  void setUp() throws Exception {
    Properties props = new Properties();
    props.put("EnableDirectResults", "0"); // Disable direct results to test CloudFetch
    props.put("CloudFetchThreadPoolSize", "1"); // Download only a small chunk.

    // Thrift is set up in the command line option.

    LOGGER.info("Setting up connection with properties: {}", props);
    connection = getValidJDBCConnection(props);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  /**
   * Test CloudFetch with multiple chunks using StreamingChunkProvider.
   *
   * <p>This test verifies:
   *
   * <ol>
   *   <li>StreamingChunkProvider is created for CloudFetch results
   *   <li>Chunks can be iterated through successfully
   *   <li>Links can be refetched from the server
   * </ol>
   */
  @Test
  void testCloudFetchLinksRefetchAtStartRowOffset() throws Exception {
    final int maxRows = 6_000_000; // Generate many chunk links.
    final String sql = "SELECT * FROM " + TABLE + " LIMIT " + maxRows;

    LOGGER.info("Executing sql query: " + sql);
    try (Statement stmt = connection.createStatement()) {
      stmt.setMaxRows(maxRows);
      stmt.execute(sql);
      ResultSet rs = stmt.getResultSet();

      LOGGER.info("Query executed, extracting chunks ...");

      // Extract the chunks that were created
      DatabricksStatement dbStatement = (DatabricksStatement) stmt;
      StatementId statementId = dbStatement.getStatementId();
      assertNotNull(statementId, "StatementId should be set after execution");

      Optional<ChunkProvider> chunkProviderOptional = ((DatabricksResultSet) rs).getChunkProvider();
      assertTrue(
          chunkProviderOptional.isPresent(),
          "Chunk provider should exist for CloudFetch result set");
      ChunkProvider chunkProvider = chunkProviderOptional.get();

      long totalChunks = chunkProvider.getChunkCount();
      assertTrue(
          totalChunks > 2, "Should have at least 3 chunks for this test, got: " + totalChunks);
      LOGGER.info("Received total chunks: {}", totalChunks);

      // Get client for refetching
      DatabricksConnection dbConnection = connection.unwrap(DatabricksConnection.class);
      IDatabricksSession session = dbConnection.getSession();
      IDatabricksClient client = session.getDatabricksClient();

      // Test refetching from the beginning (chunk 0, row offset 0)
      testRefetchLinks(statementId, 0L, 0L, client);

      // Iterate through chunks to verify they can be accessed
      int chunksProcessed = 0;
      while (chunkProvider.hasNextChunk() && chunksProcessed < 5) {
        assertTrue(chunkProvider.next(), "Should be able to advance to next chunk");
        assertNotNull(chunkProvider.getChunk(), "Chunk should not be null");
        chunksProcessed++;
        LOGGER.info("Successfully accessed chunk {}", chunksProcessed);
      }

      assertTrue(chunksProcessed > 0, "Should have processed at least one chunk");
      LOGGER.info("Total chunks processed: {}", chunksProcessed);
    }
  }

  private void testRefetchLinks(
      StatementId statementId, long chunkIndex, long chunkStartRowOffset, IDatabricksClient client)
      throws Exception {

    // Fetch from the startRowOffset of the target chunk
    Collection<ExternalLink> refetchedLinks =
        client.getResultChunks(statementId, chunkIndex, chunkStartRowOffset).getChunkLinks();

    assertNotNull(refetchedLinks, "Refetched links should not be null");
    assertFalse(refetchedLinks.isEmpty(), "Refetched links should not be empty");

    LOGGER.info("Refetched {} links from chunk index {}", refetchedLinks.size(), chunkIndex);

    // Verify each link has valid properties
    for (ExternalLink link : refetchedLinks) {
      assertNotNull(link.getExternalLink(), "Link should have a file URL");
      assertTrue(link.getRowCount() > 0, "Link should have positive row count");
    }
  }
}
