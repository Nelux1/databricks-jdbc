package com.jayant;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class TestReporter {
  private final List<ComparisonResult> results = new ArrayList<>();
  private final Path outputPath;
  private final List<String> connectionUrls = new ArrayList<>();

  public TestReporter(Path outputPath) {
    this.outputPath = outputPath;
  }

  public void addConnectionUrl(String label, String url) {
    connectionUrls.add(label + ": " + url);
  }

  public void addResult(ComparisonResult result) {
    results.add(result);
  }

  public void generateReport() throws IOException {
    try (FileWriter writer = new FileWriter(outputPath.toFile())) {
      writer.write("Report generated at: " + Instant.now() + "\n");
      for (String url : connectionUrls) {
        writer.write(url + "\n");
      }
      writer.write("============================\n\n");
      boolean hasDifferences = false;
      for (ComparisonResult result : results) {
        if (result.hasDifferences()) {
          hasDifferences = true;
          writer.write(result.toString());
          writer.write("\n============================\n\n");
        }
      }

      if (!hasDifferences) {
        writer.write("No differences found between JDBC drivers.\n");
      }
    }
  }
}
