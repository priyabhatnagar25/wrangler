package io.cdap.plugin.wrangler.actions;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.gson.JsonObject;
import org.junit.Assert;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GCSValidationHelper {
  private static final String PROJECT_ID = "cdf-athena";
  private static final String BUCKET_NAME = "wrangler-automation";
  private static final String expectedFilePath = "/Users/bharatgulati/Desktop/WRANGLER/wrangler/wrangler-core/src/e2e-test/resources/ExpectedGCS/GCS_Directive_Fillemptycells_sendtoerror-cdap-data-pipeline";

  public static void main(String[] args) {
    validateActualDataToExpectedData(expectedFilePath, BUCKET_NAME);
  }

  public static boolean validateActualDataToExpectedData(String expectedFilePath, String bucketName) {
    boolean isMatched = matchJsonList(expectedFilePath, bucketName);
    if (isMatched) {
      System.out.println("The lists are matched.");
    } else {
      System.out.println("The lists are not matched.");
    }
    return isMatched;
  }
  public static boolean matchJsonList(String expectedFilePath, String bucketName) {
    try {
      List<List<String>> expectedData = readDataFromFile(expectedFilePath);

      // Compare data from GCS with expectedData
      List<List<String>> actualData = listBucketObjects(bucketName);
      List<String> demo = new ArrayList<>();
      for (int i = 0; i < actualData.get(0).size(); i++) {
        demo.add(actualData.get(0).get(i));
      }
      int actualRowCount = demo.size();
      int expectedRowCount = expectedData.size();

      if (actualRowCount != expectedRowCount) {
        System.out.println("Row count does not match.");
      }

      for (int i = 0; i < expectedRowCount; i++) {
        List<String> expectedRow = expectedData.get(i);
        List<String> actualRow = Collections.singletonList(demo.get(i));

        if (!expectedRow.equals(actualRow)) {
          return false;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  public static List<List<String>> listBucketObjects(String bucketName) {
    List<List<String>> fileData = new ArrayList<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    Page<Blob> blobs = storage.list(bucketName);

    // Adding all the Objects which have data in a list.
    List<Blob> bucketObjects = StreamSupport.stream(blobs.iterateAll().spliterator(), true)
      .filter(blob -> blob.getSize() != 0)
      .collect(Collectors.toList());

    Stream<String> objectNamesWithData = bucketObjects.stream().map(blob -> blob.getName());
    List<String> bucketObjectNames = objectNamesWithData.collect(Collectors.toList());

    // Add the 0th index to the fileData list
    if (!bucketObjectNames.isEmpty()) {
      String objectName = bucketObjectNames.get(0);
      if (objectName.contains("part-r")) {
        List<String> objectData = fetchObjectData(PROJECT_ID, bucketName, objectName);
        fileData.add(objectData);
      }
    }

    return fileData;
  }

  private static List<String> fetchObjectData(String projectId, String bucketName, String objectName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    return Arrays.asList(objectDataAsString.split("\n"));
  }


  private static List<List<String>> readDataFromFile(String filePath) throws IOException {
    List<List<String>> data = new ArrayList<>();

    try (FileInputStream fis = new FileInputStream(filePath)) {
      byte[] buffer = new byte[fis.available()];
      fis.read(buffer);

      String fileContent = new String(buffer);
      String[] lines = fileContent.split("\\r?\\n");

      for (String line : lines) {
        List<String> row = new ArrayList<>();
        row.add(line);
        data.add(row);
      }
    }

    return data;
  }
}