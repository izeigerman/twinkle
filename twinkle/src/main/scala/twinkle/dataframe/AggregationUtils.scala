/*
 * Copyright 2017 Iaroslav Zeigerman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package twinkle.dataframe

import org.apache.spark.sql.types.{StringType, NumericType}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions._
import scala.reflect.runtime.{universe => ru}
import twinkle.sql.functions._
import AggregationUtils._

final case class AggregationUtils(groupedDataset: RelationalGroupedDataset) {

  private lazy val originalDf: DataFrame = getOriginalDataFrame

  /** Applies both numeric and string aggregators.
    *
    * @return a dataframe with aggregated values.
    */
  def aggregate: DataFrame = {
    val numericExpressions = applyAggregators(getNumericFields, NumericAggregators)
    val categoricalExpressions = applyAggregators(getStringFields, StringAggregators)
    applyExpressions(numericExpressions ++ categoricalExpressions)
  }

  /** Applies numeric aggregators only.
    *
    * @return a dataframe with aggregated values.
    */
  def aggregateNumeric: DataFrame = {
    aggregateNumeric(getNumericFields)
  }

  /** Applies numeric aggregators on a specified
    * list of columns.
    *
    * @param columns a list of numeric column names.
    * @return a dataframe with aggregated values.
    */
  def aggregateNumeric(columns: Seq[String]): DataFrame = {
    require(columns.nonEmpty, "Empty list of numeric columns")
    val expressions = applyAggregators(columns, NumericAggregators)
    applyExpressions(expressions)
  }

  /** Applies string aggregators only.
    *
    * @return a dataframe with aggregated values.
    */
  def aggregateString: DataFrame = {
    aggregateString(getStringFields)
  }

  /** Applies string aggregators on a specified
    * list of columns.
    *
    * @param columns a list of string column names.
    * @return a dataframe with aggregated values.
    */
  def aggregateString(columns: Seq[String]): DataFrame = {
    require(columns.nonEmpty, "Empty list of string columns")
    val expressions = applyAggregators(columns, StringAggregators)
    applyExpressions(expressions)
  }

  private def applyAggregators(columns: Seq[String],
                               aggregators: Seq[Aggregator]): Seq[Column] = {
    columns.flatMap(columnName =>
      aggregators.map {
        case (fName, f) => f(columnName).as(s"${columnName}_${fName}")
      }
    )
  }

  private def applyExpressions(expressions: Seq[Column]): DataFrame = {
    groupedDataset.agg(expressions.head, expressions.tail: _*)
  }

  private def getNumericFields: Seq[String] = {
    originalDf.schema.filter(_.dataType.isInstanceOf[NumericType]).map(_.name)
  }

  private def getStringFields: Seq[String] = {
    originalDf.schema.filter(_.dataType == StringType).map(_.name)
  }

  private def getOgriginalDataFrame: DataFrame = {
    val dfField = ru.typeOf[RelationalGroupedDataset]
      .members.filter(_.name.decodedName.toString == "df").head
    val im = ru.runtimeMirror(this.getClass.getClassLoader)
    im.reflect(groupedDataset).reflectField(dfField.asTerm).get.asInstanceOf[DataFrame]
  }
}

object AggregationUtils {

  private type Aggregator = (String, String => Column)

  private val NumericAggregators: Seq[Aggregator] = Seq(
    "min" -> min, "max" -> max, "mean" -> mean,
    "stddev" -> stddev, "variance" -> variance,
    "max_sub_min" -> ((col: String) => max(col) - min(col))
  )

  private val StringAggregators: Seq[Aggregator] = Seq(
    "distinct_count" -> ((col: String) => countDistinct(col)),
    "concat" -> concatAgg, "most_frequent" -> mostFrequentValue,
    "second_most_frequent" -> secondMostFrequentValue
  )
}
