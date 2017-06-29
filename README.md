# Twinkle

Twinkle - is the collection of tools and utils that can make it easier to use Apache Spark in some scenarios.

## DataFrame Utils

### Resolve the column ambiguity
In some cases it's possible to end up with DataFrame that contains multiple columns with the same name. For example this can happen as a result of `join` operation. Twinkle has a solution for this:
```scala
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
                     default: AmbiguityResolver = TakeFirstColumn): DataFrame 
```
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
```
Result:
```
+---+-------+-------+
| id|column1|column2|
+---+-------+-------+
|  0| value1| value2|
+---+-------+-------+
```
