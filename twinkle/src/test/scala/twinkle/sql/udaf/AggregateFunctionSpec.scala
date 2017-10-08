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

import org.scalatest.{FlatSpec, Matchers}
import twinkle.SparkSessionMixin

class AggregateFunctionSpec extends FlatSpec with Matchers with SparkSessionMixin {

  "ConcatAggregateFunction" should "concatenate all string values" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        (0, "string1"),
        (0, "string2"),
        (0, "string3"),
        (0, "string1")
      )).toDF("id", "str")

      val concat = ConcatAggregateFunction()
      val result = df.groupBy("id").agg(concat(df("str")).as("str_concat"))
      val record = result.select("str_concat").collect()(0)
      record.getString(0) shouldBe "string1 string2 string3"
    }
  }

  it should "indicate that too many unique values are present in the column" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        (0, "string1"),
        (0, "string2"),
        (0, "string3"),
        (0, "string1")
      )).toDF("id", "str")

      val concat = ConcatAggregateFunction(separator = " ", maxUniqueValues = 2,
        undefinedIdentifier = MapBasedCategoricalFunction.UndefinedIdentifier)
      val result = df.groupBy("id").agg(concat(df("str")).as("str_concat"))
      val record = result.select("str_concat").collect()(0)
      record.getString(0) shouldBe MapBasedCategoricalFunction.UndefinedIdentifier
    }
  }

  "FrequentItemAggregateFunction" should "return the first and the second most frequent item" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        (0, "string1"),
        (0, "string2"),
        (0, "string3"),
        (0, "string1")
      )).toDF("id", "str")

      val mostFrequent = MostFrequentValueFunction()
      val secondMostFrequent = SecondMostFrequentValueFunction()
      val result = df.groupBy("id").agg(
        mostFrequent(df("str")).as("str_first"),
        secondMostFrequent(df("str")).as("str_second"))
      val record = result.select("str_first", "str_second").collect()(0)
      record.getString(0) shouldBe "string1"
      record.getString(1) shouldBe "string2"
    }
  }

  it should "indicate that too many unique values are present in the column" in {
    withSparkSession { spark =>
      val maxUniqueValues = 100
      val rows = 10000
      val partitions = 10
      val df = spark.createDataFrame(
        (0 until rows).map(idx => (0, s"string$idx"))
      ).toDF("id", "str").repartition(partitions)

      val mostFrequent = MostFrequentValueFunction(
        maxUniqueValues, MapBasedCategoricalFunction.UndefinedIdentifier)
      val result = df.groupBy("id").agg(mostFrequent(df("str")).as("str_first"))
      val record = result.select("str_first").collect()(0)
      record.getString(0) shouldBe MapBasedCategoricalFunction.UndefinedIdentifier
    }
  }
}
