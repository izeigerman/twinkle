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
package twinkle.sql.udaf

import org.apache.spark.sql.types._
import scala.collection.Map

/** The aggregation function that concatenates all unique column values
  * into a single value.
  *
  * @param separator a separator between values.
  * @param maxUniqueValues if the number of unique values in a column exceeds
  *                        this value, the result of this function is considered
  *                        undefined.
  * @param undefinedIdentifier the optional value that will be returned by this
  *                            function in case when result is undefined. If not
  *                            specified the null reference will be returned.
  */
final case class ConcatAggregateFunction(separator: String, maxUniqueValues: Int,
                                         undefinedIdentifier: Option[String])
  extends MapBasedCategoricalFunction[Null](NullType, maxUniqueValues, undefinedIdentifier) {

  protected def initializeBuffer: Map[String, Null] = Map.empty[String, Null]

  protected def doUpdate(buffer: Map[String, Null],
                         value: String): Map[String, Null] = {
    buffer + (value -> null)
  }

  protected def doMerge(buffer1: Map[String, Null],
                        buffer2: Map[String, Null]): Map[String, Null] = {
    buffer1 ++ buffer2
  }

  protected def doEvaluate(buffer: Map[String, Null]): String = {
    buffer.toSeq.map(_._1).sorted.mkString(separator)
  }
}

object ConcatAggregateFunction {
  def apply(): ConcatAggregateFunction = {
    ConcatAggregateFunction(
      separator = " ",
      maxUniqueValues = MapBasedCategoricalFunction.DefaultMaxValues,
      undefinedIdentifier = None)
  }
}
