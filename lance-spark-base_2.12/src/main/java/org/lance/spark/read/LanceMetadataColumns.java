/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.read;

import org.lance.spark.LanceConstant;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Central registry of the metadata (virtual) columns that Lance surfaces on {@code
 * SupportsMetadataColumns}. The registry has two responsibilities that must stay in lock-step with
 * each other:
 *
 * <ol>
 *   <li>declaring the {@link MetadataColumn} instances exposed to Spark's analyzer via {@link
 *       #ALL};
 *   <li>driving the scanner-level projection in {@code LanceFragmentScanner} — each column must be
 *       excluded from the "regular columns" list and appended to the Arrow output list in the same
 *       order as {@link #ALL} so Spark's batch layout lines up with the Rust scanner's output.
 * </ol>
 *
 * Adding a new metadata column is a single-line addition below plus an {@link #ALL} entry; callers
 * do not need to touch {@code LanceDataset} or the scanner.
 */
public final class LanceMetadataColumns {

  private LanceMetadataColumns() {}

  public static final MetadataColumn ROW_ID =
      column(LanceConstant.ROW_ID, DataTypes.LongType, true);
  public static final MetadataColumn ROW_ADDRESS =
      column(LanceConstant.ROW_ADDRESS, DataTypes.LongType, false);
  public static final MetadataColumn ROW_LAST_UPDATED_AT_VERSION =
      column(LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType, true);
  public static final MetadataColumn ROW_CREATED_AT_VERSION =
      column(LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true);
  public static final MetadataColumn FRAGMENT_ID =
      column(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, false);
  public static final MetadataColumn FTS_SCORE =
      column(LanceConstant.FTS_SCORE, DataTypes.FloatType, true);

  /**
   * All metadata columns registered with Spark's analyzer. Used both by {@code
   * LanceDataset.metadataColumns()} and by {@code LanceFragmentScanner} to exclude these names from
   * the regular data-column projection.
   */
  public static final MetadataColumn[] ALL =
      new MetadataColumn[] {
        ROW_ID,
        ROW_ADDRESS,
        ROW_LAST_UPDATED_AT_VERSION,
        ROW_CREATED_AT_VERSION,
        FRAGMENT_ID,
        FTS_SCORE
      };

  /**
   * Metadata columns that flow through the native scanner's column-projection list, in the order
   * the Rust scanner emits them. Excludes columns computed per-fragment outside the scanner
   * (currently {@link #FRAGMENT_ID}, derived from the partition's fragment id).
   */
  public static final MetadataColumn[] PROJECTABLE =
      new MetadataColumn[] {
        ROW_ID, ROW_ADDRESS, ROW_LAST_UPDATED_AT_VERSION, ROW_CREATED_AT_VERSION, FTS_SCORE
      };

  private static final Set<String> ALL_NAMES =
      Collections.unmodifiableSet(
          Arrays.stream(ALL).map(MetadataColumn::name).collect(Collectors.toSet()));

  /** Returns the set of registered metadata column names, for fast membership checks. */
  public static Set<String> allNames() {
    return ALL_NAMES;
  }

  private static MetadataColumn column(String name, DataType type, boolean nullable) {
    return new MetadataColumn() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public DataType dataType() {
        return type;
      }

      @Override
      public boolean isNullable() {
        return nullable;
      }
    };
  }
}
