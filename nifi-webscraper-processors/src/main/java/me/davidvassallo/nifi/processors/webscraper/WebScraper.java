/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.davidvassallo.nifi.processors.webscraper;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"webscraper"})
@CapabilityDescription("Scrapes a website for a value")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class WebScraper extends AbstractProcessor {


    public static final PropertyDescriptor URL_PROPERTY = new PropertyDescriptor
            .Builder().name("URL_PROPERTY")
            .displayName("URL")
            .description("Enter URL from which to scrape data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_ELEMENT_PROPERTY = new PropertyDescriptor
            .Builder().name("DATA_ELEMENT_PROPERTY")
            .displayName("CSS Selector")
            .description("Enter the CSS Selector from which to scrape data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHROME_DRIVER_PROPERTY = new PropertyDescriptor
            .Builder().name("CHROME_DRIVER_PROPERTY")
            .displayName("Chrome Driver Filepath")
            .description("Enter the filepath for the chrome driver. The latest version can be downloaded from http://chromedriver.storage.googleapis.com/index.html")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("SUCCESS_RELATIONSHIP")
            .description("Success")
            .build();

    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("FAIL_RELATIONSHIP")
            .description("Failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(URL_PROPERTY);
        descriptors.add(DATA_ELEMENT_PROPERTY);
        descriptors.add(CHROME_DRIVER_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAIL_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        flowFile = session.putAttribute(flowFile, "webscraper.url", context.getProperty(URL_PROPERTY).getValue());

        try {
            System.setProperty("webdriver.chrome.driver",context.getProperty(CHROME_DRIVER_PROPERTY).getValue());
            WebDriver driver = new ChromeDriver();
            driver.get(context.getProperty(URL_PROPERTY).getValue());
            WebElement element = driver.findElement(By.cssSelector(context.getProperty(DATA_ELEMENT_PROPERTY).getValue()));
            String data = element.getText();

            driver.close();

            flowFile = session.putAttribute(flowFile, "webscraper.data", data);
            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
        } catch (Exception e){
            e.printStackTrace();
            flowFile = session.putAttribute(flowFile, "webscraper.error", e.getMessage());
            session.transfer(flowFile, FAIL_RELATIONSHIP);
        }

    }
}
