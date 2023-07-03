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
package io.cdap.plugin.wrangler.actions;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BQValidationHelper {
  private static final Gson gson = new Gson();

  private static final String fileName = "wrangler-core/src/e2e-test/resources/ExpectedBigQuery/expectedFile3";

  private static final String targetTable4 = "bqTargetTable26";

  public static void main(String[] args) throws IOException, InterruptedException {
    validateActualDataToExpectedData(fileName);
  }

  public static boolean validateActualDataToExpectedData(String fileName) throws IOException, InterruptedException {
    Map<String, JsonObject> bigQueryMap = new HashMap<>();
    Map<String, JsonObject> fileMap = new HashMap<>();

    getBigQueryTableData(targetTable4, bigQueryMap);
    getFileData(fileName, fileMap);

    boolean isMatched = matchJsonMaps(bigQueryMap, fileMap);

    if (isMatched) {
      System.out.println("The data is matched.");
    } else {
      System.out.println("The data is not matched.");
    }

    return isMatched;
  }

  public static void getFileData(String fileName, Map<String, JsonObject> fileMap) {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonObject json = gson.fromJson(line, JsonObject.class);
        String id = json.get("ID").getAsString();
        fileMap.put(id, json);
      }
    } catch (IOException e) {
      System.err.println("Error reading the file: " + e.getMessage());
    }
  }

  private static void getBigQueryTableData(String targetTable, Map<String, JsonObject> bigQueryMap)
    throws IOException, InterruptedException {
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + targetTable + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

    for (FieldValueList row : result.iterateAll()) {
      JsonObject json = gson.fromJson(row.get(0).getStringValue(), JsonObject.class);
      JsonElement idElement = json.get("ID");
      if (idElement != null && idElement.isJsonPrimitive()) {
        String id = idElement.getAsString(); // Replace "id" with the actual key in the JSON
        bigQueryMap.put(id, json);
      } else {
        System.out.println("Data Mismatched");
      }
    }
    System.out.println(bigQueryMap);
  }

  private static boolean matchJsonMaps(Map<String, JsonObject> map1, Map<String, JsonObject> map2) {
    if (!map1.keySet().equals(map2.keySet())) {
      return false;
    }
    for (String key : map1.keySet()) {
      JsonObject json1 = map1.get(key);
      JsonObject json2 = map2.get(key);
      if (!json1.equals(json2)) {
        return false;
      }
    }
    return true;
  }
}
