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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import scala.collection.Map

/** The strategy that determines which column has to be picked up from
  * within multiple column indexes.
  */
trait AmbiguityResolver {
  /** Selects one column index from within given sequence of indexes.
    *
    * @param indexes the sequence of column indexes.
    * @return the selected column index.
    */
  def select(indexes: Seq[Int]): Int
}

object TakeFirstColumn extends AmbiguityResolver {
  override def select(indexes: Seq[Int]): Int = {
    indexes.min
  }
}

object TakeLastColumn extends AmbiguityResolver {
  override def select(indexes: Seq[Int]): Int = {
    indexes.max
  }
}

case class AmbiguousColumnsUtils(inputDf: DataFrame) {

  /** Resolves the column ambiguity using the provided strategies for
    * each column name.
    *
    * @param strategies the map of strategies where the key is the column
    *                   name and the value is a strategy implementation.
    * @param default the fallback strategy in case when a column is not found
    *                in strategies.
    * @return a new DataFrame that contains no ambiguous columns.
    */
  def resolveAmbiguity(strategies: Map[String, AmbiguityResolver],
                       default: AmbiguityResolver): DataFrame = {
    val strategiesLowerCase = strategies.map(i => i._1.toLowerCase() -> i._2)
    val columns = inputDf.columns.map(_.toLowerCase).zipWithIndex
    val groupedColumns: Map[String, Array[Int]] = columns.groupBy(_._1).mapValues(_.map(_._2))

    val selectedIndexes = groupedColumns
      .map {
        case (name, indexes) =>
          val strategy = strategiesLowerCase.getOrElse(name, default)
          strategy.select(indexes)
      }.toSeq.sorted

    val schema: StructType = inputDf.schema
    val newSchema: StructType = StructType(selectedIndexes.map(schema(_)))

    implicit val encoder = RowEncoder(newSchema)
    val outputDf = inputDf.map(row => Row.fromSeq(selectedIndexes.map(row.get(_))))
    outputDf
  }

  /** Resolves the column ambiguity using the provided per column strategies.
    * If a column is not specified the [[TakeFirstColumn]] strategy will be used.
    *
    * @param strategies the map of strategies where the key is the column
    *                   name and the value is a strategy implementation.
    * @return a new DataFrame that contains no ambiguous columns.
    */
  def resolveAmbiguity(strategies: Map[String, AmbiguityResolver]): DataFrame = {
    resolveAmbiguity(strategies, TakeFirstColumn)
  }

  /** Applies a given strategy to all ambiguous columns in the DataFrame.
    *
    * @param strategy the strategy that has to be applied.
    * @return a new DataFrame that contains no ambiguous columns.
    */
  def resolveAmbiguity(strategy: AmbiguityResolver): DataFrame = {
    resolveAmbiguity(Map.empty, strategy)
  }

  /** Applies the [[TakeFirstColumn]] strategy to all ambiguous  columns
    * in the DataFrame.
    *
    * @return a new DataFrame that contains no ambiguous columns.
    */
  def resolveAmbiguity(): DataFrame = {
    resolveAmbiguity(Map.empty, TakeFirstColumn)
  }

  /** Renames the ambiguous columns using the mapping from the
    * column's index to its new name.
    *
    * @param renamedColumns a mapping of columns indexes to their
    *                       new names.
    * @return a new DataFrame with renamed columns.
    */
  def renameAmbiguousColumns(renamedColumns: Map[Int, String]): DataFrame = {
    val schema: StructType = inputDf.schema
    val renamedFields: Seq[StructField] = schema.zipWithIndex.map {
      case (field, index) if renamedColumns.contains(index) =>
        field.copy(name = renamedColumns(index))
      case (field, _) => field
    }
    val newSchema = StructType(renamedFields)
    implicit val encoder = RowEncoder(newSchema)
    inputDf.map(identity)
  }

  /** Same as [[renameAmbiguousColumns(Map[Int, String])]] version but takes
    * a variable number of arguments instead.
    *
    * @param renamedColumns a mapping of columns indexes to their
    *                       new names.
    * @return a new DataFrame with renamed columns.
    */
  def renameAmbiguousColumns(renamedColumns: (Int, String)*): DataFrame = {
    renameAmbiguousColumns(renamedColumns.toMap)
  }
}
