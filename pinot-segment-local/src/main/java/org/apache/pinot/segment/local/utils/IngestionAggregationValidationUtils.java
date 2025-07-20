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

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;


/**
 * Utility class for validating ingestion aggregation configurations.
 * This class provides shared validation logic that can be used both during
 * table config validation and segment creation.
 */
public final class IngestionAggregationValidationUtils {
  private IngestionAggregationValidationUtils() {
  }

  /**
   * Validates that the schema and indexing configuration are compatible with metrics aggregation.
   * This validation ensures that:
   * 1. All metric columns are no-dictionary and single-value
   * 2. All dimension columns are dictionary-encoded and single-value
   * 3. All time columns are dictionary-encoded
   * 
   * @param schema The schema to validate
   * @param indexingConfig The indexing configuration to validate
   * @param tableName The table name for error messages
   * @throws IllegalStateException if validation fails
   */
  public static void validateMetricsAggregationCompatibility(Schema schema, IndexingConfig indexingConfig, 
      String tableName) {
    Map<String, DictionaryIndexConfig> configPerCol = StandardIndexes.dictionary().getConfig(null, schema);
    Set<String> noDictionaryColumns = new HashSet<>();
    for (Map.Entry<String, DictionaryIndexConfig> entry : configPerCol.entrySet()) {
      if (entry.getValue().isDisabled()) {
        noDictionaryColumns.add(entry.getKey());
      }
    }

    // All metric columns should have no-dictionary index and be single value
    for (FieldSpec fieldSpec : schema.getMetricFieldSpecs()) {
      String metric = fieldSpec.getName();
      if (!noDictionaryColumns.contains(metric)) {
        throw new IllegalStateException(
            String.format("Metrics aggregation cannot be enabled in presence of dictionary encoded metrics: %s for table: %s",
                metric, tableName));
      }

      if (!fieldSpec.isSingleValueField()) {
        throw new IllegalStateException(
            String.format("Metrics aggregation cannot be enabled in presence of multi-value metric columns: %s for table: %s",
                metric, tableName));
      }
    }

    // All dimension columns should be dictionary encoded and single value
    for (FieldSpec fieldSpec : schema.getDimensionFieldSpecs()) {
      String dimension = fieldSpec.getName();
      if (noDictionaryColumns.contains(dimension)) {
        throw new IllegalStateException(
            String.format("Metrics aggregation cannot be enabled in presence of no-dictionary dimensions: %s for table: %s",
                dimension, tableName));
      }

      if (!fieldSpec.isSingleValueField()) {
        throw new IllegalStateException(
            String.format("Metrics aggregation cannot be enabled in presence of multi-value dimension columns: %s for table: %s",
                dimension, tableName));
      }
    }

    // Time columns should be dictionary encoded
    if (schema.getTimeFieldSpec() != null) {
      String timeColumn = schema.getTimeFieldSpec().getName();
      if (noDictionaryColumns.contains(timeColumn)) {
        throw new IllegalStateException(
            String.format("Metrics aggregation cannot be enabled in presence of no-dictionary datetime/time columns: %s for table: %s",
                timeColumn, tableName));
      }
    }
  }

  /**
   * Checks if metrics aggregation is compatible with the given schema and indexing configuration.
   * This method performs the same validation as {@link #validateMetricsAggregationCompatibility}
   * but returns a boolean instead of throwing an exception.
   * 
   * @param schema The schema to check
   * @param indexingConfig The indexing configuration to check
   * @return true if metrics aggregation is compatible, false otherwise
   */
  public static boolean isMetricsAggregationCompatible(Schema schema, IndexingConfig indexingConfig) {
    try {
      validateMetricsAggregationCompatibility(schema, indexingConfig, "unknown");
      return true;
    } catch (IllegalStateException e) {
      return false;
    }
  }

  /**
   * Validates that all metric columns in the schema are configured as no-dictionary columns
   * in the indexing configuration. This is required for ingestion aggregation to work properly.
   * 
   * @param schema The schema containing the metric columns
   * @param indexingConfig The indexing configuration to validate
   * @param tableName The table name for error messages
   * @throws IllegalStateException if any metric column is not configured as no-dictionary
   */
  public static void validateAllMetricsAreNoDictionary(Schema schema, IndexingConfig indexingConfig, String tableName) {
    Map<String, DictionaryIndexConfig> configPerCol = StandardIndexes.dictionary().getConfig(null, schema);
    
    for (FieldSpec fieldSpec : schema.getMetricFieldSpecs()) {
      String metric = fieldSpec.getName();
      DictionaryIndexConfig dictConfig = configPerCol.get(metric);
      if (dictConfig == null || !dictConfig.isDisabled()) {
        throw new IllegalStateException(
            String.format("Aggregated column: %s must be a no-dictionary column for table: %s", metric, tableName));
      }
    }
  }
} 
