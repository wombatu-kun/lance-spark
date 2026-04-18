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
package org.lance.spark.read

import org.apache.spark.sql.execution.datasources.v2.VectorIndexParamsBuilder
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.index.DistanceType

class VectorIndexParamsBuilderTest {

  // ─── DistanceTypes ─────────────────────────────────────────────────────────

  @Test def distanceTypesAcceptsAllAliases(): Unit = {
    assertEquals(DistanceType.L2, DistanceTypes.parse("l2", "ctx"))
    assertEquals(DistanceType.L2, DistanceTypes.parse("L2", "ctx"))
    assertEquals(DistanceType.L2, DistanceTypes.parse("euclidean", "ctx"))
    assertEquals(DistanceType.Cosine, DistanceTypes.parse("cosine", "ctx"))
    assertEquals(DistanceType.Cosine, DistanceTypes.parse("Cosine", "ctx"))
    assertEquals(DistanceType.Dot, DistanceTypes.parse("dot", "ctx"))
    assertEquals(DistanceType.Dot, DistanceTypes.parse("inner_product", "ctx"))
    assertEquals(DistanceType.Dot, DistanceTypes.parse("ip", "ctx"))
    assertEquals(DistanceType.Hamming, DistanceTypes.parse("hamming", "ctx"))
  }

  @Test def distanceTypesRejectsUnknown(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => DistanceTypes.parse("manhattan", "myctx"))
    assertTrue(ex.getMessage.contains("myctx"))
    assertTrue(ex.getMessage.contains("manhattan"))
  }

  // ─── VectorIndexParamsBuilder ──────────────────────────────────────────────

  @Test def isVectorMethodRecognisesSupportedKinds(): Unit = {
    assertTrue(VectorIndexParamsBuilder.isVectorMethod("ivf_flat"))
    assertTrue(VectorIndexParamsBuilder.isVectorMethod("IVF_PQ"))
    assertTrue(VectorIndexParamsBuilder.isVectorMethod("ivf_hnsw_pq"))
    assertTrue(VectorIndexParamsBuilder.isVectorMethod("ivf_hnsw_sq"))
    assertFalse(VectorIndexParamsBuilder.isVectorMethod("btree"))
    assertFalse(VectorIndexParamsBuilder.isVectorMethod("fts"))
  }

  @Test def buildHandlesEmptyArgs(): Unit = {
    Seq(null.asInstanceOf[String], "", "  ", "{}").foreach { json =>
      val params = VectorIndexParamsBuilder.build("ivf_flat", json)
      assertNotNull(params, s"build(ivf_flat, '$json') returned null")
    }
  }

  @Test def buildAcceptsMixedNumericTypes(): Unit = {
    val json =
      """{"num_partitions": 4, "sample_rate": 256, "num_sub_vectors": 2.0, "num_bits": 8}"""
    val params = VectorIndexParamsBuilder.build("ivf_pq", json)
    assertNotNull(params)
  }

  @Test def buildRejectsNonIntegerForIntArg(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => VectorIndexParamsBuilder.build("ivf_pq", """{"num_partitions": "four"}"""))
    assertTrue(ex.getMessage.contains("num_partitions"))
  }

  @Test def buildRejectsUnknownMethod(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => VectorIndexParamsBuilder.build("hnsw", "{}"))
    assertTrue(ex.getMessage.toLowerCase.contains("unsupported vector index method"))
  }

  @Test def buildRejectsBadMetric(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => VectorIndexParamsBuilder.build("ivf_flat", """{"metric": "manhattan"}"""))
    assertTrue(ex.getMessage.contains("manhattan"))
  }

  @Test def buildRejectsNonObjectJson(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => VectorIndexParamsBuilder.build("ivf_flat", "[1, 2, 3]"))
    assertTrue(ex.getMessage.toLowerCase.contains("json object"))
  }
}
