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
package org.apache.spark.sql.execution.datasources.v2

import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.parse
import org.lance.index.DistanceType
import org.lance.index.vector.{HnswBuildParams, IvfBuildParams, PQBuildParams, SQBuildParams, VectorIndexParams}
import org.lance.spark.read.DistanceTypes

/**
 * Builds a [[VectorIndexParams]] from the user-supplied `WITH (...)` arguments serialised
 * as a JSON object (see [[IndexUtils.toJson]]).
 *
 * The scalar path can just call `ScalarIndexParams.create(method, json)` — Lance parses the
 * JSON itself. The vector path has no equivalent factory, so the arguments must be parsed
 * explicitly and threaded through the per-kind builder APIs.
 *
 * Recognised keys (all optional; defaults come from Lance):
 *   - metric          : "l2" | "cosine" | "dot" | "hamming"
 *   - num_partitions  : Int                    (IVF)
 *   - sample_rate     : Int                    (IVF / PQ / SQ)
 *   - max_iterations  : Int                    (IVF / PQ)
 *   - num_sub_vectors : Int                    (PQ)
 *   - num_bits        : Int                    (PQ or SQ; method-dependent)
 *   - m               : Int                    (HNSW)
 *   - ef_construction : Int                    (HNSW)
 */
object VectorIndexParamsBuilder {

  def isVectorMethod(method: String): Boolean = method.toLowerCase match {
    case "ivf_flat" | "ivf_pq" | "ivf_hnsw_pq" | "ivf_hnsw_sq" => true
    case _ => false
  }

  def build(method: String, argsJson: String): VectorIndexParams = {
    val args = parseArgs(argsJson)
    val distance = parseDistance(args.get("metric"))
    val ivf = buildIvf(args)

    val builder = new VectorIndexParams.Builder(ivf).setDistanceType(distance)

    method.toLowerCase match {
      case "ivf_flat" =>
        // nothing extra — IVF centroids alone are enough for a flat index
        ()
      case "ivf_pq" =>
        builder.setPqParams(buildPq(args))
      case "ivf_hnsw_pq" =>
        builder.setHnswParams(buildHnsw(args))
        builder.setPqParams(buildPq(args))
      case "ivf_hnsw_sq" =>
        builder.setHnswParams(buildHnsw(args))
        builder.setSqParams(buildSq(args))
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported vector index method: '$other'. " +
            "Expected one of: ivf_flat, ivf_pq, ivf_hnsw_pq, ivf_hnsw_sq")
    }

    builder.build()
  }

  private def parseArgs(argsJson: String): Map[String, JValue] = {
    if (argsJson == null || argsJson.trim.isEmpty || argsJson.trim == "{}") {
      return Map.empty
    }
    parse(argsJson) match {
      case JObject(fields) => fields.toMap
      case other =>
        throw new IllegalArgumentException(
          s"Expected JSON object for index arguments, got: $other")
    }
  }

  private def parseDistance(value: Option[JValue]): DistanceType = value match {
    case None | Some(JNull) => DistanceType.L2
    case Some(JString(s)) => DistanceTypes.parse(s, "vector index")
    case Some(other) =>
      throw new IllegalArgumentException(
        s"Metric must be a string, got: $other")
  }

  private def buildIvf(args: Map[String, JValue]): IvfBuildParams = {
    val b = new IvfBuildParams.Builder()
    asInt(args, "num_partitions").foreach(b.setNumPartitions)
    asInt(args, "sample_rate").foreach(b.setSampleRate)
    asInt(args, "max_iterations").foreach(b.setMaxIters)
    b.build()
  }

  private def buildPq(args: Map[String, JValue]): PQBuildParams = {
    val b = new PQBuildParams.Builder()
    asInt(args, "num_sub_vectors").foreach(b.setNumSubVectors)
    asInt(args, "num_bits").foreach(b.setNumBits)
    asInt(args, "sample_rate").foreach(b.setSampleRate)
    asInt(args, "max_iterations").foreach(b.setMaxIters)
    b.build()
  }

  private def buildSq(args: Map[String, JValue]): SQBuildParams = {
    val b = new SQBuildParams.Builder()
    asInt(args, "num_bits").foreach(n => b.setNumBits(n.toShort))
    asInt(args, "sample_rate").foreach(b.setSampleRate)
    b.build()
  }

  private def buildHnsw(args: Map[String, JValue]): HnswBuildParams = {
    val b = new HnswBuildParams.Builder()
    asInt(args, "m").foreach(b.setM)
    asInt(args, "ef_construction").foreach(b.setEfConstruction)
    b.build()
  }

  private def asInt(args: Map[String, JValue], key: String): Option[Int] =
    args.get(key).flatMap {
      case JInt(n) => Some(n.toInt)
      case JLong(n) => Some(n.toInt)
      case JDouble(d) if d == d.toInt => Some(d.toInt)
      case JDecimal(d) if d.isValidInt => Some(d.toInt)
      case JNull => None
      case other =>
        throw new IllegalArgumentException(
          s"Index argument '$key' must be an integer, got: $other")
    }
}
