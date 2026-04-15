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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.ipc.Query
import org.lance.spark.{LanceDataset, LanceDataSource, LanceSparkReadOptions}
import org.lance.spark.utils.QueryUtils

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * `lance_vector_search` — table-valued function exposing Lance ANN / kNN pushdown to Spark SQL.
 *
 * Usage (positional; first four required):
 * {{{
 *   SELECT id, category
 *   FROM lance_vector_search(
 *          'lance.db.items',            -- table ref (catalog-qualified id OR filesystem URI)
 *          'embedding',                 -- vector column
 *          array(0.1f, 0.2f, ...),      -- query vector (float / double array)
 *          10                           -- k
 *          [, 'cosine'                  -- metric: l2 | cosine | dot | hamming
 *          [, 20                        -- nprobes (IVF)
 *          [, 1                         -- refine_factor (PQ)
 *          [, 64                        -- ef (HNSW)
 *          [, true                      -- use_index (false = brute force)
 *          ]]]]]
 *        )
 *   WHERE category = 'books'
 *   ORDER BY _distance;                  -- only if column was projected through by the scan
 * }}}
 *
 * Named arguments (Spark 3.5+) are also accepted and recognised by parameter name:
 *   `table`, `column`, `query`, `k`, `metric`, `nprobes`, `refine_factor`, `ef`, `use_index`.
 *
 * The resulting LogicalPlan is a standard Lance DataSource read with the
 * [[LanceSparkReadOptions.CONFIG_NEAREST]] option populated — so all existing
 * pushdown, filter, and projection paths apply unchanged.
 */
object LanceVectorSearchTableFunction {

  val NAME = "lance_vector_search"

  val IDENTIFIER: FunctionIdentifier = FunctionIdentifier(NAME)

  val INFO: ExpressionInfo = new ExpressionInfo(
    "org.lance.spark.read.LanceVectorSearchTableFunction",
    null,
    NAME,
    "_FUNC_(table, column, query, k" +
      "[, metric[, nprobes[, refine_factor[, ef[, use_index]]]]]) - " +
      "Approximate nearest-neighbour search over a Lance vector column.",
    "",
    """
      |    Examples:
      |      > SELECT id, _distance FROM _FUNC_('lance.db.items', 'embedding',
      |          array(0.1f, 0.2f, 0.3f), 10, 'cosine');
    """.stripMargin,
    "",
    "table_funcs",
    "",
    "",
    "built-in")

  val BUILDER: Seq[Expression] => LogicalPlan = (args: Seq[Expression]) => buildPlan(args)

  /**
   * Core builder. Separated from [[BUILDER]] to make unit testing straightforward.
   */
  def buildPlan(args: Seq[Expression]): LogicalPlan = {
    val parsed = parseArgs(args)

    val query = buildQuery(parsed)
    val queryJson = QueryUtils.queryToString(query)

    val spark = SparkSession.active
    val (datasetUri, storageOptions) = resolveDatasetLocation(spark, parsed.table)

    val reader = spark.read.format(LanceDataSource.name)
    // Apply catalog-derived storage options first so the caller-facing `nearest`
    // cannot be clobbered by per-table config.
    storageOptions.foreach { case (k, v) => reader.option(k, v) }
    reader.option(LanceSparkReadOptions.CONFIG_NEAREST, queryJson)

    val analyzed = reader.load(datasetUri).queryExecution.analyzed
    // Wrap the underlying LanceDataset in a decorator that surfaces the virtual `_distance`
    // column in the relation's schema. Done here (not in `LanceDataSource.getTable`) because
    // `SupportsCatalogOptions` routes `.load()` through `catalog.loadTable(ident)`, which
    // bypasses `getTable` and never sees the per-read `nearest` option.
    analyzed.transformUp {
      case rel: DataSourceV2Relation if rel.table.isInstanceOf[LanceDataset] =>
        val wrapped = new LanceVectorSearchTable(rel.table.asInstanceOf[LanceDataset])
        DataSourceV2Relation.create(wrapped, rel.catalog, rel.identifier, rel.options)
    }
  }

  // ─── Argument parsing ──────────────────────────────────────────────────────

  private[read] case class ParsedArgs(
      table: String,
      column: String,
      query: Array[Float],
      k: Int,
      metric: Option[String],
      nprobes: Option[Int],
      refineFactor: Option[Int],
      ef: Option[Int],
      useIndex: Option[Boolean])

  /** Test-only hook so unit tests can exercise argument parsing without a SparkSession. */
  private[read] def parseArgsForTest(args: Seq[Expression]): ParsedArgs = parseArgs(args)

  private def parseArgs(args: Seq[Expression]): ParsedArgs = {
    val byName = scala.collection.mutable.Map.empty[String, Expression]
    val positional = scala.collection.mutable.ArrayBuffer.empty[Expression]
    args.foreach { expr =>
      NamedArgExtractor.unapply(expr) match {
        case Some((name, value)) => byName(name.toLowerCase) = value
        case None => positional += expr
      }
    }

    def atName(name: String): Option[Expression] = byName.get(name)
    def atPos(idx: Int): Option[Expression] =
      if (idx < positional.length) Some(positional(idx)) else None
    def pick(name: String, idx: Int): Option[Expression] = atName(name).orElse(atPos(idx))

    val tableExpr = pick("table", 0)
      .getOrElse(throw missing("table"))
    val columnExpr = pick("column", 1)
      .getOrElse(throw missing("column"))
    val queryExpr = pick("query", 2)
      .getOrElse(throw missing("query"))
    val kExpr = pick("k", 3)
      .getOrElse(throw missing("k"))

    val table = evalString(tableExpr, "table")
    val column = evalString(columnExpr, "column")
    val queryVec = evalFloatArray(queryExpr, "query")
    val k = evalInt(kExpr, "k")
    require(k > 0, s"lance_vector_search: 'k' must be positive, got $k")

    val metric = pick("metric", 4).map(evalString(_, "metric"))
    val nprobes = pick("nprobes", 5).map(evalInt(_, "nprobes"))
    val refineFactor = pick("refine_factor", 6).map(evalInt(_, "refine_factor"))
    val ef = pick("ef", 7).map(evalInt(_, "ef"))
    val useIndex = pick("use_index", 8).map(evalBoolean(_, "use_index"))

    ParsedArgs(table, column, queryVec, k, metric, nprobes, refineFactor, ef, useIndex)
  }

  private def missing(name: String): IllegalArgumentException =
    new IllegalArgumentException(s"lance_vector_search: missing required argument '$name'")

  private def evalLiteral(expr: Expression, argName: String): Any = {
    val folded = expr match {
      case l: Literal => l
      case other if other.foldable => Literal(other.eval(), other.dataType)
      case other =>
        throw new IllegalArgumentException(
          s"lance_vector_search: argument '$argName' must be a constant expression, got $other")
    }
    folded.value
  }

  private def evalString(expr: Expression, argName: String): String =
    evalLiteral(expr, argName) match {
      case null =>
        throw new IllegalArgumentException(s"lance_vector_search: '$argName' cannot be null")
      case s: UTF8String => s.toString
      case s: String => s
      case other =>
        throw new IllegalArgumentException(
          s"lance_vector_search: '$argName' must be a STRING literal, got ${other.getClass.getName}")
    }

  private def evalInt(expr: Expression, argName: String): Int = evalLiteral(expr, argName) match {
    case null =>
      throw new IllegalArgumentException(s"lance_vector_search: '$argName' cannot be null")
    case i: java.lang.Integer => i.intValue()
    case i: Int => i
    case l: java.lang.Long => l.intValue()
    case other =>
      throw new IllegalArgumentException(
        s"lance_vector_search: '$argName' must be an integer, got ${other.getClass.getName}")
  }

  private def evalBoolean(expr: Expression, argName: String): Boolean =
    evalLiteral(expr, argName) match {
      case null =>
        throw new IllegalArgumentException(s"lance_vector_search: '$argName' cannot be null")
      case b: java.lang.Boolean => b.booleanValue()
      case b: Boolean => b
      case other =>
        throw new IllegalArgumentException(
          s"lance_vector_search: '$argName' must be a BOOLEAN, got ${other.getClass.getName}")
    }

  private def evalFloatArray(expr: Expression, argName: String): Array[Float] = {
    val value = evalLiteral(expr, argName)
    value match {
      case null =>
        throw new IllegalArgumentException(s"lance_vector_search: '$argName' cannot be null")
      case arr: ArrayData =>
        val dt = expr.dataType match {
          case org.apache.spark.sql.types.ArrayType(elementType, _) => elementType
          case other =>
            throw new IllegalArgumentException(
              s"lance_vector_search: '$argName' must be an ARRAY of numeric values, got $other")
        }
        def checkNotNull(i: Int): Unit =
          if (arr.isNullAt(i)) {
            throw new IllegalArgumentException(
              s"lance_vector_search: '$argName' must not contain null elements (index $i)")
          }
        dt match {
          case FloatType =>
            val out = new Array[Float](arr.numElements())
            var i = 0
            while (i < out.length) {
              checkNotNull(i)
              out(i) = arr.getFloat(i)
              i += 1
            }
            out
          case DoubleType =>
            val out = new Array[Float](arr.numElements())
            var i = 0
            while (i < out.length) {
              checkNotNull(i)
              out(i) = arr.getDouble(i).toFloat
              i += 1
            }
            out
          case IntegerType | LongType =>
            val out = new Array[Float](arr.numElements())
            var i = 0
            while (i < out.length) {
              checkNotNull(i)
              out(i) = if (dt == IntegerType) arr.getInt(i).toFloat else arr.getLong(i).toFloat
              i += 1
            }
            out
          case other =>
            throw new IllegalArgumentException(
              s"lance_vector_search: '$argName' must be ARRAY<FLOAT|DOUBLE>, " +
                s"got ARRAY<$other>")
        }
      case other =>
        throw new IllegalArgumentException(
          s"lance_vector_search: '$argName' must be an ARRAY literal, got ${other.getClass.getName}")
    }
  }

  // ─── Query assembly ───────────────────────────────────────────────────────

  private def buildQuery(p: ParsedArgs): Query = {
    val b = new Query.Builder()
      .setColumn(p.column)
      .setKey(p.query)
      .setK(p.k)
      .setUseIndex(p.useIndex.getOrElse(true))
    p.metric.foreach(m => b.setDistanceType(DistanceTypes.parse(m, "lance_vector_search")))
    p.nprobes.foreach(b.setMinimumNprobes)
    p.refineFactor.foreach(b.setRefineFactor)
    p.ef.foreach(b.setEf)
    b.build()
  }

  // ─── Table resolution ─────────────────────────────────────────────────────

  /**
   * Resolves a user-supplied table reference to a Lance dataset URI plus any storage options
   * inherited from the catalog. Accepts either a catalog-qualified name (e.g. `lance.db.t`) or a
   * plain filesystem URI. Catalog lookup uses [[SparkSession.table]] and walks the analysed plan
   * for a [[LanceDataset]].
   */
  private def resolveDatasetLocation(
      spark: SparkSession,
      tableRef: String): (String, Map[String, String]) = {
    val trimmed = tableRef.trim
    // Heuristic: only treat as a filesystem URI if it carries a scheme or an absolute/relative
    // path prefix. A bare `.lance` suffix is *not* enough — a catalog identifier may legitimately
    // end in `.lance` (e.g. `cat.db.my.lance`).
    val looksLikeUri = trimmed.contains("://") || trimmed.startsWith("/") ||
      trimmed.startsWith("./") || trimmed.startsWith("../")
    if (looksLikeUri) {
      return (trimmed, Map.empty)
    }
    val plan =
      try {
        spark.table(trimmed).queryExecution.analyzed
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"lance_vector_search: could not resolve table '$tableRef' " +
              "(treat it as a catalog identifier or a Lance URI).",
            e)
      }
    val lanceTable = plan.collectFirst {
      case rel: DataSourceV2Relation if rel.table.isInstanceOf[LanceDataset] =>
        rel.table.asInstanceOf[LanceDataset]
    }.getOrElse(throw new IllegalArgumentException(
      s"lance_vector_search: table '$tableRef' does not resolve to a Lance dataset."))
    val readOpts = lanceTable.readOptions()
    val storage = Option(readOpts.getStorageOptions)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    (readOpts.getDatasetUri, storage)
  }

  /**
   * Extracts a name → expression pair from a [[NamedArgumentExpression]] in Spark 3.5+.
   * Uses reflection so this file compiles against older Spark versions where the class does not
   * exist — those versions simply never produce such expressions, so the extractor returns None.
   */
  private object NamedArgExtractor {
    private val clazz: Class[_] =
      try {
        Class.forName("org.apache.spark.sql.catalyst.analysis.NamedArgumentExpression")
      } catch {
        case _: ClassNotFoundException => null
      }
    private val keyMethod: java.lang.reflect.Method =
      if (clazz == null) null else clazz.getMethod("key")
    private val valueMethod: java.lang.reflect.Method =
      if (clazz == null) null else clazz.getMethod("value")

    def unapply(expr: Expression): Option[(String, Expression)] = {
      if (clazz == null || !clazz.isInstance(expr)) {
        None
      } else {
        Some((
          keyMethod.invoke(expr).asInstanceOf[String],
          valueMethod.invoke(expr).asInstanceOf[Expression]))
      }
    }
  }
}
