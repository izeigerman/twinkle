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

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import twinkle.SparkSessionMixin

class AmbiguousColumnsUtilsSpec extends FlatSpec with Matchers with SparkSessionMixin {

  "AmbiguousColumnsUtils" should "resolve ambiguous columns using strategies" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        ("col1_1", "col2_1", "col3_1", "col4_1", "col5_1", "col6_1", "col7_1", "col8_1")
      )).toDF("name1", "Name1", "name1", "name2", "NaMe2", "name3", "NAME3", "name4")

      val strategies = Map(
        "NAME1" -> TakeFirstColumn,
        "NAME2" -> TakeLastColumn
      )

      val result = AmbiguousColumnsUtils(df).resolveAmbiguity(strategies, default = TakeFirstColumn)

      val expectedColumns = Array("name1", "NaMe2", "name3", "name4")
      result.columns shouldBe expectedColumns

      val expectedRow = Row.fromSeq(Seq("col1_1", "col5_1", "col6_1", "col8_1"))
      result.collect()(0) shouldBe expectedRow
    }
  }

  it should "rename ambiguous columns" in {
    withSparkSession { spark =>
      val df = spark.createDataFrame(Seq(
        ("col1_1", "col2_1", "col3_1", "col4_1", "col5_1")
      )).toDF("name1", "Name1", "name2", "NaMe2", "name3")

      val mapping = Map(
        1 -> "name4",
        3 -> "name5"
      )
      val result = AmbiguousColumnsUtils(df).renameAmbiguousColumns(mapping)

      val expectedColumns = Array("name1", "name4", "name2", "name5", "name3")
      result.columns shouldBe expectedColumns

      val expectedRow = Row.fromSeq(Seq("col1_1", "col2_1", "col3_1", "col4_1", "col5_1"))
      result.collect()(0) shouldBe expectedRow
    }
  }
}
