/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.utils;

import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


public class IngestionAggregationValidationUtilsTest {

  @Test
  public void testValidateMetricsAggregationCompatibility() {
    // Test case 1: Valid configuration - all metrics are no-dictionary and single-value
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addSingleValueDimension("dim2", FieldSpec.DataType.INT)
        .addMetric("metric1", FieldSpec.DataType.DOUBLE)
        .addMetric("metric2", FieldSpec.DataType.LONG)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "timeColumn"), null)
        .build();

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Arrays.asList("metric1", "metric2"));

    // Should not throw exception
    IngestionAggregationValidationUtils.validateMetricsAggregationCompatibility(schema, indexingConfig, "testTable");
  }

  @Test
  public void testValidateMetricsAggregationCompatibilityWithDictionaryMetrics() {
    // Test case 2: Invalid configuration - metric has dictionary
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.DOUBLE)
        .build();

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Collections.emptyList()); // metric1 has dictionary

    try {
      IngestionAggregationValidationUtils.validateMetricsAggregationCompatibility(schema, indexingConfig, "testTable");
      Assert.fail("Should fail due to metric having dictionary");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("dictionary encoded metrics"));
    }
  }

  @Test
  public void testValidateMetricsAggregationCompatibilityWithMultiValueMetrics() {
    // Test case 3: Invalid configuration - multi-value metric
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMultiValueDimension("metric1", FieldSpec.DataType.DOUBLE) // This will be treated as dimension
        .build();

    // Add metric field manually to make it multi-value
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("metric2", FieldSpec.DataType.DOUBLE);
    metricFieldSpec.setSingleValueField(false); // Make it multi-value
    schema.addField(metricFieldSpec);

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Arrays.asList("metric2"));

    try {
      IngestionAggregationValidationUtils.validateMetricsAggregationCompatibility(schema, indexingConfig, "testTable");
      Assert.fail("Should fail due to multi-value metric");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("multi-value metric columns"));
    }
  }

  @Test
  public void testValidateMetricsAggregationCompatibilityWithNoDictionaryDimensions() {
    // Test case 4: Invalid configuration - dimension has no-dictionary
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.DOUBLE)
        .build();

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Arrays.asList("metric1", "dim1")); // dim1 has no-dictionary

    try {
      IngestionAggregationValidationUtils.validateMetricsAggregationCompatibility(schema, indexingConfig, "testTable");
      Assert.fail("Should fail due to dimension having no-dictionary");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("no-dictionary dimensions"));
    }
  }

  @Test
  public void testValidateAllMetricsAreNoDictionary() {
    // Test case 5: Valid configuration - all metrics are no-dictionary
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addMetric("metric1", FieldSpec.DataType.DOUBLE)
        .addMetric("metric2", FieldSpec.DataType.LONG)
        .build();

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Arrays.asList("metric1", "metric2"));

    // Should not throw exception
    IngestionAggregationValidationUtils.validateAllMetricsAreNoDictionary(schema, indexingConfig, "testTable");
  }

  @Test
  public void testValidateAllMetricsAreNoDictionaryWithDictionaryMetric() {
    // Test case 6: Invalid configuration - metric has dictionary
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addMetric("metric1", FieldSpec.DataType.DOUBLE)
        .addMetric("metric2", FieldSpec.DataType.LONG)
        .build();

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Arrays.asList("metric1")); // metric2 has dictionary

    try {
      IngestionAggregationValidationUtils.validateAllMetricsAreNoDictionary(schema, indexingConfig, "testTable");
      Assert.fail("Should fail due to metric having dictionary");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("must be a no-dictionary column"));
    }
  }

  @Test
  public void testIsMetricsAggregationCompatible() {
    // Test case 7: Test the boolean check method
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testTable")
        .addSingleValueDimension("dim1", FieldSpec.DataType.STRING)
        .addMetric("metric1", FieldSpec.DataType.DOUBLE)
        .build();

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setNoDictionaryColumns(Arrays.asList("metric1"));

    // Should return true for valid configuration
    Assert.assertTrue(IngestionAggregationValidationUtils.isMetricsAggregationCompatible(schema, indexingConfig));

    // Should return false for invalid configuration
    indexingConfig.setNoDictionaryColumns(Collections.emptyList());
    Assert.assertFalse(IngestionAggregationValidationUtils.isMetricsAggregationCompatible(schema, indexingConfig));
  }
} 
