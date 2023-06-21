package io.cdap.plugin.wrangler.stepsdesign;
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


import io.cdap.e2e.pages.actions.CdfConnectionActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.wrangler.actions.WranglerPropertiesPageActions;
import io.cdap.plugin.wrangler.locators.WranglerPropertiesPage;
import io.cucumber.java.en.Then;
import org.apache.commons.lang3.RandomStringUtils;
import org.openqa.selenium.WebElement;
import scala.xml.Elem;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static stepsdesign.PipelineSteps.pipelineName;


public class Wrangler implements CdfHelper {

    @Then("Click on the Plus Green Button to import the pipelines")
    public void clickOnPlusGreenImportButton () {
        WranglerPropertiesPageActions.clickPlusGreenImportButton();
    }

    @Then("Select the json files for importing the pipelines {string}")
    public void selectJSONFiles(String path) throws URISyntaxException {
        WranglerPropertiesPageActions.importJsonFiles(PluginPropertyUtils.pluginProp(path));
    }

    @Then("Rename the pipeline")
    public void saveThePipeline() {
        WaitHelper.waitForElementToBeOptionallyDisplayed(WranglerPropertiesPage.renamePipeline(),100);
        ElementHelper.clickOnElement(WranglerPropertiesPage.appendPipeline);
        pipelineName = "TestPipeline-" + RandomStringUtils.randomAlphanumeric(10);
        WranglerPropertiesPageActions.fillPipelineNameAndSave(pipelineName);
    }
}
