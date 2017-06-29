# Twinkle

Twinkle - is the collection of tools and utils that can make it easier to use Apache Spark in some scenarios.

## DataFrame Utils

### Resolve the column ambiguity
In some cases it's possible to end up with DataFrame that contains multiple columns with the same name. For example this can happen as a result of `join` operation. Twinkle has a [solution](https://github.com/izeigerman/twinkle/blob/master/twinkle/src/main/scala/twinkle/dataframe/AmbiguousColumnsUtils.scala) for this:
```scala
val df1 = spark.createDataFrame(Seq(
  (0, "value1")
)).toDF("id", "column1")

val df2 = spark.createDataFrame(Seq(
  (0, "value2")
)).toDF("id", "column2")

val joined = df1.join(df2, df1("id") === df2("id"), "inner")

import twinkle._
joined.resolveAmbiguity().show()

// or

joined.renameAmbiguousColumns(2 -> "id2").show()

```
Result:
```
+---+-------+-------+
| id|column1|column2|
+---+-------+-------+
|  0| value1| value2|
+---+-------+-------+

or

+---+-------+---+-------+
| id|column1|id2|column2|
+---+-------+---+-------+
|  0| value1|  0| value2|
+---+-------+---+-------+
```
