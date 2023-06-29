/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.UUID;

import static io.cdap.e2e.pages.locators.CdfGCSLocators.filePath;

/**
 * BQ test hooks.
 */
public class TestSetupHooks {
  public static String bqSourceTable1 = StringUtils.EMPTY;
  public static String gcsTargetBucketName = StringUtils.EMPTY;

  @Before(order = 1, value = "@BQ_SINK_TEST")
  public static void setTempTargetBQTableName() {
    String bqTargetTableName = "E2E_TARGET_" + UUID.randomUUID().toString().replaceAll("-", "_");
    PluginPropertyUtils.addPluginProp("bqTargetTable", bqTargetTableName);
    BeforeActions.scenario.write("BQ Target table name - " + bqTargetTableName);
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("bqTargetTable");
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("bqTargetTable");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  /**
   * Create BigQuery table.
   */
  @Before(order = 1, value = "@BQ_SOURCE_TEST")
  public static void createTempSourceBQTable() throws IOException, InterruptedException {
    createSourceBQTableWithQueries(PluginPropertyUtils.pluginProp("CreateBQTableQueryFile"),
                                   PluginPropertyUtils.pluginProp("InsertBQDataQueryFile"));
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST")
  public static void deleteTempSourceBQTable() throws IOException, InterruptedException {
    String bqSourceTable = PluginPropertyUtils.pluginProp("bqSourceTable");
    BigQueryClient.dropBqQuery(bqSourceTable);
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable + " deleted successfully");
    PluginPropertyUtils.removePluginProp("bqSourceTable");
  }

  private static void createSourceBQTableWithQueries(String bqCreateTableQueryFile, String bqInsertDataQueryFile)
    throws IOException, InterruptedException {
    String bqSourceTable = "E2E_SOURCE_" + UUID.randomUUID().toString().substring(0, 5).replaceAll("-",
            "_");

    String createTableQuery = StringUtils.EMPTY;
    try {
      createTableQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
              ("/" + bqCreateTableQueryFile).toURI()))
              , StandardCharsets.UTF_8);
      createTableQuery = createTableQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
              .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqCreateTableQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
              "- error in reading create table query file " + e.getMessage());
    }

    String insertDataQuery = StringUtils.EMPTY;
    try {
      insertDataQuery = new String(Files.readAllBytes(Paths.get(TestSetupHooks.class.getResource
              ("/" + bqInsertDataQueryFile).toURI()))
              , StandardCharsets.UTF_8);
      insertDataQuery = insertDataQuery.replace("DATASET", PluginPropertyUtils.pluginProp("dataset"))
              .replace("TABLE_NAME", bqSourceTable);
    } catch (Exception e) {
      BeforeActions.scenario.write("Exception in reading " + bqInsertDataQueryFile + " - " + e.getMessage());
      Assert.fail("Exception in BigQuery testdata prerequisite setup " +
              "- error in reading insert data query file " + e.getMessage());
    }
    BigQueryClient.getSoleQueryResult(createTableQuery);
    try {
      BigQueryClient.getSoleQueryResult(insertDataQuery);
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    PluginPropertyUtils.addPluginProp("bqSourceTable", bqSourceTable);
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable + " created successfully");
  }

  @Before(order = 1, value = "@BQ_SOURCE_TEST1")
  public static void createTempPartitionedSourceBQTable() throws IOException, InterruptedException {
    bqSourceTable1 = "E2E_SOURCE_" + UUID.randomUUID().toString().replaceAll("-", "_");
    BigQueryClient.getSoleQueryResult("create table `test_automation." + bqSourceTable1 + "` " +
            "(Customer_id INT64, First_name STRING, Last_name STRING, Age INT64, " +
            "Address STRING, Col1 STRING, create_date DATE, update_date DATE, body STRING)");
    try {
      BigQueryClient.getSoleQueryResult("INSERT INTO `test_automation." + bqSourceTable1 + "` " +
              "(Customer_id, First_name, Last_name, Age, Address, Col1, create_date, update_date, body)" +
              " VALUES (1, 'Shelby','Shelbylastname', 20, 'Address1', 'testdata', '2021-01-28', '2021-01-30', 'Test' )");
    } catch (NoSuchElementException e) {
      // Insert query does not return any record.
      // Iterator on TableResult values in getSoleQueryResult method throws NoSuchElementException
    }
    BeforeActions.scenario.write("BQ Source Table " + bqSourceTable1 + " created successfully");
  }

  @After(order = 1, value = "@BQ_SOURCE_TEST1")
  public static void deleteTempSourceBQTable1() throws IOException, InterruptedException {
    BigQueryClient.dropBqQuery(bqSourceTable1);
    PluginPropertyUtils.removePluginProp("bqSourceTable");
    BeforeActions.scenario.write("BQ source Table " + bqSourceTable1 + " deleted successfully");
    bqSourceTable1 = StringUtils.EMPTY;
  }



  @Before(order = 1, value = "@GCS_SINK_TEST1")
    public static void setTempTargetGCSBucketName1() {
      gcsTargetBucketName = "cdf-e2e-test-" + UUID.randomUUID();
      PluginPropertyUtils.addPluginProp("gcsTargetBucketName", gcsTargetBucketName);
      PluginPropertyUtils.addPluginProp("gcsTargetPath", "gs://" + gcsTargetBucketName);
      BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
    }

    @After(order = 1, value = "@GCS_SINK_TEST1")
    public static void deleteTargetBucketWithFile1() {
      deleteGCSBucket(gcsTargetBucketName);
      PluginPropertyUtils.removePluginProp("gcsTargetBucketName");
      PluginPropertyUtils.removePluginProp("gcsTargetPath");
      gcsTargetBucketName = StringUtils.EMPTY;
    }

  @Before(order = 1, value = "@GCS_SINK_TEST")
  public static void setTempTargetGCSBucketName() throws IOException {
    gcsTargetBucketName = createGCSBucket();
    PluginPropertyUtils.addPluginProp("gcsTargetBucket", "gs://" + gcsTargetBucketName);
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST")
  public static void deleteTargetBucketWithFile() {
    deleteGCSBucket(gcsTargetBucketName);
    gcsTargetBucketName = StringUtils.EMPTY;
  }

  @Before(order = 1, value = "@GCS_SINK_TEST2")
  public static void setTempTargetGCSBucketName2() {
    String gcsTargetBucketName = "cdf-e2e-test-" + UUID.randomUUID();
    PluginPropertyUtils.addPluginProp("gcsTargetBucketName", gcsTargetBucketName);
    PluginPropertyUtils.addPluginProp("gcsTargetPath", "gs://" + gcsTargetBucketName);
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST2")
  public static void deleteTargetBucketWithFile2() {
    deleteGCSBucket(PluginPropertyUtils.pluginProp("gcsTargetBucketName"));
    PluginPropertyUtils.removePluginProp("gcsTargetBucketName");
    PluginPropertyUtils.removePluginProp("gcsTargetPath");
  }

  private static String createGCSBucket() throws IOException {
    return StorageClient.createBucket("e2e-test-" + UUID.randomUUID()).getName();
  }
    private static void deleteGCSBucket(String bucketName) {
      try {
        for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
          StorageClient.deleteObject(bucketName, blob.getName());
        }
        StorageClient.deleteBucket(bucketName);
        BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
      } catch (StorageException | IOException e) {
        if (e.getMessage().contains("The specified bucket does not exist")) {
          BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
        } else {
          Assert.fail(e.getMessage());
        }
      }
    }
      }
