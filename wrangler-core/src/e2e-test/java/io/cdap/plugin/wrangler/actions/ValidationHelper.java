package io.cdap.plugin.wrangler.actions;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ValidationHelper {

    public static void main(String[] args) {
      String actualFilePath = "/Users/bharatgulati/Desktop/WRANGLER/wrangler/wrangler-core/src/e2e-test/resources/ExpectedGCS/Expected_DATA";
      String expectedFilePath = "/Users/bharatgulati/Desktop/WRANGLER/wrangler/wrangler-core/src/e2e-test/resources/ExpectedGCS/Expected_DATA";
      listBucketObjects("testdata-filesink");

      try {
        List<List<String>> actualData = readDataFromFile(actualFilePath);
        List<List<String>> expectedData = readDataFromFile(expectedFilePath);

        // Compare column count
        int actualColumnCount = actualData.get(0).size();
        int expectedColumnCount = expectedData.get(0).size();
        if (actualColumnCount != expectedColumnCount) {
          System.out.println("Column count does not match.");
        }

        // Compare row count
        int actualRowCount = actualData.size() - 1; // Subtracting header row
        int expectedRowCount = expectedData.size() - 1; // Subtracting header row
        if (actualRowCount != expectedRowCount) {
          System.out.println("Row count does not match.");
          return;
        }

        // Compare data inside columns
        for (int i = 1; i < actualData.size(); i++) {
          List<String> actualRow = actualData.get(i);
          List<String> expectedRow = expectedData.get(i);
          if (!actualRow.equals(expectedRow)) {
            System.out.println("Data mismatch in row: " + (i + 1));
            return;
          }
        }

        System.out.println("Data matches in both files.");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  private static String[] fetchObjectData(String projectId, String bucketName, String objectName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    return objectDataAsString.split("\n");
  }


  public static void listBucketObjects(String bucketName) {
    List<String> bucketObjectNames = new ArrayList<>();
    Storage storage = StorageOptions.newBuilder().setProjectId("cdf-athena").build().getService();
    Page<Blob> blobs = storage.list(bucketName);

    // Adding all the Objects which have data in a list.
    List<Blob> bucketObjects = StreamSupport.stream(blobs.iterateAll().spliterator(), true)
      .filter(blob -> blob.getSize() != 0)
      .collect(Collectors.toList());

    Stream<String> objectNamesWithData = bucketObjects.stream().map(blob -> blob.getName());
    objectNamesWithData.forEach(objectName -> bucketObjectNames.add(objectName));
    System.out.println(bucketObjectNames.get(0));
  }
    private static List<List<String>> readDataFromFile(String filePath) throws IOException {
      List<List<String>> data = new ArrayList<>();

      try (FileInputStream fis = new FileInputStream(filePath)) {
        byte[] buffer = new byte[fis.available()];
        fis.read(buffer);

        String fileContent = new String(buffer);
        String[] lines = fileContent.split("\\r?\\n");

        for (String line : lines) {
          List<String> row = Arrays.asList(line.split(","));
          data.add(row);
        }
      }

      return data;
    }
}
