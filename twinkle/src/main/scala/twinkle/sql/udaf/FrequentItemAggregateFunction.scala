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

class FrequentItemAggregateFunction(position: Int,
                                    maxUniqueValues: Int,
                                    undefinedIdentifier: String)
  extends MapBasedCategoricalFunction[Long](LongType, maxUniqueValues, undefinedIdentifier) {

  override protected def initializeBuffer: Map[String, Long] = Map.empty[String, Long]

  override protected def doUpdate(buffer: Map[String, Long],
                                  value: String): Map[String, Long] = {
    val counter = buffer.getOrElse(value, 0L)
    buffer + (value -> (counter + 1))
  }

  override protected def doMerge(buffer1: Map[String, Long],
                                 buffer2: Map[String, Long]): Map[String, Long] = {
    val keys = buffer1.keySet ++ buffer2.keySet
    keys.map(key => {
      (key -> (buffer1.getOrElse(key, 0L) + buffer2.getOrElse(key, 0L)))
    }).toMap
  }

  override protected def doEvaluate(buffer: Map[String, Long]): String = {
    buffer.toIndexedSeq.sortBy(i => (-i._2, i._1)).apply(position)._1
  }
}

final case class MostFrequentValueFunction(maxUniqueValues: Int, undefinedIdentifier: String)
  extends FrequentItemAggregateFunction(0, maxUniqueValues, undefinedIdentifier)

object MostFrequentValueFunction {
  def apply(): MostFrequentValueFunction = {
    MostFrequentValueFunction(MapBasedCategoricalFunction.DefaultMaxValues,
      MapBasedCategoricalFunction.UndefinedIdentifier)
  }
}

final case class SecondMostFrequentValueFunction(maxUniqueValues: Int, undefinedIdentifier: String)
  extends FrequentItemAggregateFunction(1, maxUniqueValues, undefinedIdentifier)

object SecondMostFrequentValueFunction {
  def apply(): SecondMostFrequentValueFunction = {
    SecondMostFrequentValueFunction(MapBasedCategoricalFunction.DefaultMaxValues,
      MapBasedCategoricalFunction.UndefinedIdentifier)
  }
}
