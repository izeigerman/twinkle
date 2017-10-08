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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{Matchers, FlatSpec}
import twinkle.SparkSessionMixin

class AggregationUtilsSpec extends FlatSpec with Matchers with SparkSessionMixin {

  private def createTestDataFrame(spark: SparkSession): DataFrame = {
    spark.createDataFrame(Seq(
      (1, 1, "string1"),
      (1, 2, "string2"),
      (1, 3, "string1")
    )).toDF("id", "num", "str")
  }

  private def validateNumeric(df: DataFrame): Unit = {
    val record = df.select("num_min", "num_max", "num_mean", "num_stddev",
      "num_variance", "num_max_sub_min").collect()(0)
    record.toSeq shouldBe Seq(1, 3, 2.0, 1.0, 1.0, 2)
  }

  private def validateCategorical(df: DataFrame): Unit = {
    val record = df
      .select("str_distinct_count", "str_concat", "str_most_frequent", "str_second_most_frequent")
      .collect()(0)
    record.toSeq shouldBe Seq(2, "string1 string2", "string1", "string2")
  }

  "AggregationUtils" should "aggregate numeric columns" in {
    withSparkSession { spark =>
      val df = createTestDataFrame(spark)
      val resultDf = new AggregationUtils(df.groupBy("id")).aggregateNumeric
      validateNumeric(resultDf)
    }
  }

  it should "aggregate categorical columns" in {
    withSparkSession { spark =>
      val df = createTestDataFrame(spark)
      val resultDf = new AggregationUtils(df.groupBy("id")).aggregateCategorical
      validateCategorical(resultDf)
    }
  }

  it should "aggregate both numeric and categorical columns" in {
    withSparkSession { spark =>
      val df = createTestDataFrame(spark)
      val resultDf = new AggregationUtils(df.groupBy("id")).aggregate
      validateCategorical(resultDf)
      validateNumeric(resultDf)
    }
  }

  it should "handle missing numeric columns" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        ("id", "string1", "string2")
      )).toDF("id", "str1", "str2")
      intercept[IllegalArgumentException] {
        new AggregationUtils(df.groupBy("id")).aggregateNumeric
      }
    }
  }

  it should "handle missing categorical columns" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        (0, 1, 2)
      )).toDF("id", "num1", "num2")
      intercept[IllegalArgumentException] {
        new AggregationUtils(df.groupBy("id")).aggregateCategorical
      }
    }
  }
}
