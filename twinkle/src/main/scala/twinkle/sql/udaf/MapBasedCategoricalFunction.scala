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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scala.collection.Map

abstract class MapBasedCategoricalFunction[T](valueType: DataType,
                                              maxUniqueValues: Int,
                                              undefinedIdentifier: Option[String])
  extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(
    StructField("value", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("buffer", DataTypes.createMapType(StringType, valueType)) ::
    StructField("isUndefined", BooleanType) ::
    Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = initializeBuffer
    buffer(1) = false
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val isUndefined = buffer.getBoolean(1)
    if (!isUndefined) {
      val result = doUpdate(getMapBuffer(buffer), Option(input.getString(0)).getOrElse("null"))
      if (result.size > maxUniqueValues) {
        buffer(1) = true
      } else {
        buffer(0) = result
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val isUndefined = buffer1.getBoolean(1) || buffer2.getBoolean(1)
    if (!isUndefined) {
      val result = doMerge(getMapBuffer(buffer1), getMapBuffer(buffer2))
      if (result.size > maxUniqueValues) {
        buffer1(1) = true
      } else {
        buffer1(0) = result
      }
    } else {
      buffer1(1) = true
    }
  }

  override def evaluate(buffer: Row): Any = {
    val isUndefined = buffer.getBoolean(1)
    if (!isUndefined) {
      doEvaluate(getMapBuffer(buffer))
    } else {
      undefinedIdentifier.orNull
    }
  }

  protected def initializeBuffer: Map[String, T]

  protected def doUpdate(buffer: Map[String, T], value: String): Map[String, T]

  protected def doMerge(buffer1: Map[String, T], buffer2: Map[String, T]): Map[String, T]

  protected def doEvaluate(buffer: Map[String, T]): String

  private def getMapBuffer(buffer: MutableAggregationBuffer): Map[String, T] = {
    buffer.getMap[String, T](0)
  }

  private def getMapBuffer(buffer: Row): Map[String, T] = {
    buffer.getMap[String, T](0)
  }
}

object MapBasedCategoricalFunction {
  val DefaultMaxValues: Int = 100
  val UndefinedIdentifier: String = "undefined"
}
