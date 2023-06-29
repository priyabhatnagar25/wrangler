# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@Wrangler
Feature:  Wrangler - Run time scenarios

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the copy count and delete directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_copy_drop_count_setcolmn"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  @BQ_SOURCE_TEST @GCS_SINK_TEST2
  Scenario: To verify User is able to run a pipeline using the fill null and send to error directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_Fillempty_sendtoerror"
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "path" with value: "gcsTargetBucketName"
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using the Format,concatenate,title case and copy column directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_Concatenate_titlecase"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using the Uppercase and Lowercase directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_lowercase_uppercase"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the find and replace,copy column and calculate length directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_FindReplace_copy"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using the Trim Spaces and parse as csv directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_parsecsv_trim"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using cut character,extract regex,filter,row true directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_cutcharacter_extractregex_filter_rowtrue"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: To verify User is able to run a pipeline using cleanse column, current date and title case directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_titlecase_cleanse_currentdate"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "table" with value: "bqSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using mask number and mask shuffle directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_maskno_maskshuffle_merge"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using parse date and generate uuid directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_parsedate_generateuuid"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  @BQ_SOURCE_TEST1
  Scenario: To verify User is able to run a pipeline using decode different date and null empty directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_decode_diffdate_nullempty"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "table" with value: "bqSourceTable1"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "table" with value: "bqTargetTable"
    Then Close the Plugin Properties page
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using flatten and format as currency directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_flatten_formatascurrency"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using hash and keep column directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_hash_keep"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline swap and join column directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_swapcolumn_joincolumn"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using parse xml as json directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_parse-xml-to-json"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using encode base 32 base 64 and set variable directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_encodebase32_encodebase64-setvariable"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using parse as length,increment variable and split rows directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_parseaslength_splitrows_incrementvariable"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs

  Scenario: To verify User is able to run a pipeline using parse as simple date directives in the wrangler plugin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Plus Green Button to import the pipelines
    Then Select the json files for importing the pipelines "Directive_parse_simpledate"
    Then Rename the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs


