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
import org.apache.spark.sql.types.StructType
import scala.collection.Map

case class DataFrameUtils(inputDf: DataFrame) {

  /** Resolves the column ambiguity using the provided strategies for
    * each column name.
    *
    * @param strategies the map of strategies where the key is the column
    *                   name and the value is a strategy implementation.
    * @param default the fallback strategy in case when a column is not found
    *                in strategies.
    * @return a new DataFrame that contains no ambiguous columns.
    */
  def resolveAmbiguity(strategies: Map[String, AmbiguityResolver] = Map.empty,
                       default: AmbiguityResolver = TakeFirstColumn): DataFrame = {
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

}
