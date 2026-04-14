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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.datasources.v2.VectorIndexParamsBuilder
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.lance.index.DistanceType

class VectorSearchArgParsingTest {

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

  // ─── LanceVectorSearchTableFunction.parseArgsForTest ───────────────────────

  private def floatArrayLit(vs: Float*): Literal =
    Literal.create(new GenericArrayData(vs.toArray[Any]), ArrayType(FloatType))

  @Test def parseArgsAcceptsRequiredPositional(): Unit = {
    val args = Seq(
      Literal("cat.db.t"),
      Literal("emb"),
      floatArrayLit(0.1f, 0.2f, 0.3f),
      Literal(5))
    val parsed = LanceVectorSearchTableFunction.parseArgsForTest(args)
    assertEquals("cat.db.t", parsed.table)
    assertEquals("emb", parsed.column)
    assertArrayEquals(Array(0.1f, 0.2f, 0.3f), parsed.query)
    assertEquals(5, parsed.k)
    assertTrue(parsed.metric.isEmpty)
    assertTrue(parsed.useIndex.isEmpty)
  }

  @Test def parseArgsAcceptsOptionalPositional(): Unit = {
    val args = Seq(
      Literal("t"),
      Literal("c"),
      floatArrayLit(1.0f),
      Literal(3),
      Literal("cosine"),
      Literal(20),
      Literal(2),
      Literal(64),
      Literal(false))
    val parsed = LanceVectorSearchTableFunction.parseArgsForTest(args)
    assertEquals(Some("cosine"), parsed.metric)
    assertEquals(Some(20), parsed.nprobes)
    assertEquals(Some(2), parsed.refineFactor)
    assertEquals(Some(64), parsed.ef)
    assertEquals(Some(false), parsed.useIndex)
  }

  @Test def parseArgsConvertsArrayDoubleAndIntQueries(): Unit = {
    val asDouble = Literal.create(
      new GenericArrayData(Array[Any](0.1d, 0.2d, 0.3d)),
      ArrayType(DoubleType))
    val pd = LanceVectorSearchTableFunction.parseArgsForTest(
      Seq(Literal("t"), Literal("c"), asDouble, Literal(3)))
    assertEquals(3, pd.query.length)
    assertTrue(math.abs(pd.query(0) - 0.1f) < 1e-6f)

    val asInt = Literal.create(
      new GenericArrayData(Array[Any](1, 2, 3)),
      ArrayType(IntegerType))
    val pi = LanceVectorSearchTableFunction.parseArgsForTest(
      Seq(Literal("t"), Literal("c"), asInt, Literal(3)))
    assertArrayEquals(Array(1.0f, 2.0f, 3.0f), pi.query)

    val asLong = Literal.create(
      new GenericArrayData(Array[Any](1L, 2L)),
      ArrayType(LongType))
    val pl = LanceVectorSearchTableFunction.parseArgsForTest(
      Seq(Literal("t"), Literal("c"), asLong, Literal(3)))
    assertArrayEquals(Array(1.0f, 2.0f), pl.query)
  }

  @Test def parseArgsRejectsNullElements(): Unit = {
    val withNull = Literal.create(
      new GenericArrayData(Array[Any](0.1f, null, 0.3f)),
      ArrayType(FloatType))
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        LanceVectorSearchTableFunction.parseArgsForTest(
          Seq(Literal("t"), Literal("c"), withNull, Literal(3))))
    assertTrue(ex.getMessage.contains("null"))
  }

  @Test def parseArgsRejectsNonPositiveK(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        LanceVectorSearchTableFunction.parseArgsForTest(
          Seq(Literal("t"), Literal("c"), floatArrayLit(1.0f), Literal(0))))
    assertTrue(ex.getMessage.contains("positive"))
  }

  @Test def parseArgsReportsMissingArgByName(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () =>
        LanceVectorSearchTableFunction.parseArgsForTest(
          Seq(Literal("t"), Literal("c"), floatArrayLit(1.0f))))
    assertTrue(ex.getMessage.contains("'k'"))
  }
}
