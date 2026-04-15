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
import org.lance.spark.LanceDataset;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.connector.catalog.SupportsMetadataColumns;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Read-only decorator around {@link LanceDataset} that surfaces the virtual {@code _distance}
 * column in {@link #schema()} so Spark's analyzer can resolve references to it in {@code SELECT},
 * {@code ORDER BY}, and {@code WHERE}. Applied when {@code nearest} is set on the read options
 * (vector-search path).
 *
 * <p>The {@code _distance} column is auto-appended by the Lance native scanner at the tail of each
 * Arrow batch when {@code ScanOptions.nearest(...)} is set; appending it to the Spark schema in the
 * same position keeps {@code requiredSchema} → Arrow column layout aligned without changes to the
 * batch reader.
 */
public class LanceVectorSearchTable implements SupportsRead, SupportsMetadataColumns {

  private static final Set<TableCapability> CAPABILITIES =
      Collections.singleton(TableCapability.BATCH_READ);

  private final LanceDataset inner;
  private final StructType augmentedSchema;

  public LanceVectorSearchTable(LanceDataset inner) {
    this.inner = inner;
    this.augmentedSchema =
        inner
            .schema()
            .add(
                new StructField(
                    LanceConstant.DISTANCE, DataTypes.FloatType, false, Metadata.empty()));
  }

  @Override
  public String name() {
    return inner.name() + "[vector_search]";
  }

  @Override
  public StructType schema() {
    return augmentedSchema;
  }

  @Override
  public Map<String, String> properties() {
    return inner.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    // Delegate to the inner dataset. Spark's V2ScanRelationPushDown always invokes
    // pruneColumns(requiredSchema) before build(), where requiredSchema is derived from this
    // Table's schema() — so the augmented schema (including _distance) is propagated through
    // the scan builder, scan, and input partition without further intervention.
    return inner.newScanBuilder(options);
  }

  @Override
  public MetadataColumn[] metadataColumns() {
    return inner.metadataColumns();
  }
}
