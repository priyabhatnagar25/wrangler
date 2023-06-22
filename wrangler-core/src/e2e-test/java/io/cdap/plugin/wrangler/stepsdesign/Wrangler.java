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
import io.cdap.e2e.utils.*;
import io.cdap.plugin.wrangler.actions.WranglerPropertiesPageActions;
import io.cdap.plugin.wrangler.locators.WranglerPropertiesPage;
import io.cucumber.java.en.Then;
import org.apache.commons.lang3.RandomStringUtils;
import org.openqa.selenium.WebElement;
import scala.xml.Elem;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Wrangler implements CdfHelper {

    static {
        SeleniumHelper.getPropertiesLocators(WranglerPropertiesPage.class);
    }

    @Then("Rename the pipeline")
    public void renameThePipeline() {
        WranglerPropertiesPageActions.renameThePipeline();
    }
}
