/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.spark.sql.TypedImperativeAggregateSuite.TypedMax
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, GenericInternalRow, ImplicitCastInputTypes, SpecificInternalRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TypedImperativeAggregateSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val random = new java.util.Random()

  private val data = (0 until 1000).map { _ =>
    (random.nextInt(10), random.nextInt(100))
  }

  test("aggregate with object aggregate buffer") {
    val agg = new TypedMax(BoundReference(0, IntegerType, nullable = false))

    val group1 = (0 until data.length / 2)
    val group1Buffer = agg.createAggregationBuffer()
    group1.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group1Buffer, input)
    }

    val group2 = (data.length / 2 until data.length)
    val group2Buffer = agg.createAggregationBuffer()
    group2.foreach { index =>
      val input = InternalRow(data(index)._1, data(index)._2)
      agg.update(group2Buffer, input)
    }

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    assert(mergeBuffer.value == data.map(_._1).max)
    assert(agg.eval(mergeBuffer) == data.map(_._1).max)

    // Tests low level eval(row: InternalRow) API.
    val row = new GenericInternalRow(Array(mergeBuffer): Array[Any])

    // Evaluates directly on row consist of aggregation buffer object.
    assert(agg.eval(row) == data.map(_._1).max)
  }

  test("supports SpecificMutableRow as mutable row") {
    val aggregationBufferSchema = Seq(IntegerType, LongType, BinaryType, IntegerType)
    val aggBufferOffset = 2
    val buffer = new SpecificInternalRow(aggregationBufferSchema)
    val agg = new TypedMax(BoundReference(ordinal = 1, dataType = IntegerType, nullable = false))
      .withNewMutableAggBufferOffset(aggBufferOffset)

    agg.initialize(buffer)
    data.foreach { kv =>
      val input = InternalRow(kv._1, kv._2)
      agg.update(buffer, input)
    }
    assert(agg.eval(buffer) == data.map(_._2).max)
  }

  test("dataframe aggregate with object aggregate buffer, should not use HashAggregate") {
    val df = data.toDF("a", "b")
    val max = TypedMax($"a".expr)

    // Always uses SortAggregateExec
    val sparkPlan = df.select(Column(max.toAggregateExpression())).queryExecution.sparkPlan
    assert(!sparkPlan.isInstanceOf[HashAggregateExec])
  }

  test("dataframe aggregate with object aggregate buffer, no group by") {
    val df = data.toDF("key", "value").coalesce(2)
    val query = df.select(typedMax($"key"), count($"key"), typedMax($"value"), count($"value"))
    val maxKey = data.map(_._1).max
    val countKey = data.size
    val maxValue = data.map(_._2).max
    val countValue = data.size
    val expected = Seq(Row(maxKey, countKey, maxValue, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, non-nullable aggregator") {
    val df = data.toDF("key", "value").coalesce(2)

    // Test non-nullable typedMax
    val query = df.select(typedMax(lit(null)), count($"key"), typedMax(lit(null)),
      count($"value"))

    // typedMax is not nullable
    val maxNull = Int.MinValue
    val countKey = data.size
    val countValue = data.size
    val expected = Seq(Row(maxNull, countKey, maxNull, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, nullable aggregator") {
    val df = data.toDF("key", "value").coalesce(2)

    // Test nullable nullableTypedMax
    val query = df.select(nullableTypedMax(lit(null)), count($"key"), nullableTypedMax(lit(null)),
      count($"value"))

    // nullableTypedMax is nullable
    val maxNull = null
    val countKey = data.size
    val countValue = data.size
    val expected = Seq(Row(maxNull, countKey, maxNull, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregation with object aggregate buffer, input row contains null") {

    val nullableData = (0 until 1000).map {id =>
      val nullableKey: Integer = if (random.nextBoolean()) null else random.nextInt(100)
      val nullableValue: Integer = if (random.nextBoolean()) null else random.nextInt(100)
      (nullableKey, nullableValue)
    }

    val df = nullableData.toDF("key", "value").coalesce(2)
    val query = df.select(typedMax($"key"), count($"key"), typedMax($"value"),
      count($"value"))
    val maxKey = nullableData.map(_._1).filter(_ != null).max
    val countKey = nullableData.map(_._1).count(_ != null)
    val maxValue = nullableData.map(_._2).filter(_ != null).max
    val countValue = nullableData.map(_._2).count(_ != null)
    val expected = Seq(Row(maxKey, countKey, maxValue, countValue))
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, with group by") {
    val df = data.toDF("value", "key").coalesce(2)
    val query = df.groupBy($"key").agg(typedMax($"value"), count($"value"), typedMax($"value"))
    val expected = data.groupBy(_._2).toSeq.map { group =>
      val (key, values) = group
      val valueMax = values.map(_._1).max
      val countValue = values.size
      Row(key, valueMax, countValue, valueMax)
    }
    checkAnswer(query, expected)
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, no group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.select(typedMax($"a"), count($"a"), typedMax($"b"), count($"b")),
      Seq(Row(Int.MinValue, 0, Int.MinValue, 0)))
  }

  test("dataframe aggregate with object aggregate buffer, empty inputs, with group by") {
    val empty = Seq.empty[(Int, Int)].toDF("a", "b")
    checkAnswer(
      empty.groupBy($"b").agg(typedMax($"a"), count($"a"), typedMax($"a")),
      Seq.empty[Row])
  }

  test("TypedImperativeAggregate should not break Window function") {
    val df = data.toDF("key", "value")
    // OVER (PARTITION BY a ORDER BY b ROW BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    val w = Window.orderBy("value").partitionBy("key").rowsBetween(Long.MinValue, 0)

    val query = df.select(sum($"key").over(w), typedMax($"key").over(w), sum($"value").over(w),
      typedMax($"value").over(w))

    val expected = data.groupBy(_._1).toSeq.flatMap { group =>
      val (key, values) = group
      val sortedValues = values.map(_._2).sorted

      var outputRows = Seq.empty[Row]
      var i = 0
      while (i < sortedValues.size) {
        val unboundedPrecedingAndCurrent = sortedValues.slice(0, i + 1)
        val sumKey = key * unboundedPrecedingAndCurrent.size
        val maxKey = key
        val sumValue = unboundedPrecedingAndCurrent.sum
        val maxValue = unboundedPrecedingAndCurrent.max

        outputRows :+= Row(sumKey, maxKey, sumValue, maxValue)
        i += 1
      }

      outputRows
    }
    checkAnswer(query, expected)
  }

  private def typedMax(column: Column): Column = {
    val max = TypedMax(column.expr, nullable = false)
    Column(max.toAggregateExpression())
  }

  private def nullableTypedMax(column: Column): Column = {
    val max = TypedMax(column.expr, nullable = true)
    Column(max.toAggregateExpression())
  }

  ignore("test attribution") {
    val oneWeek = 604800000 // 7 * 60 * 60 * 24 * 1000
    val colNames = Seq("uid", "eid", "dim1", "dim2", "measure1", "measure2", "ts")

    // test attribution models
    {
      Seq(
        (1, 1, null, null, 0, 0, 1),
        (1, 2, null, null, 0, 0, 2),
        (1, 3, null, null, 0, 0, 3),
        (1, 4, null, null, 0, 0, 1 + oneWeek),
        (1, 5, null, null, 100, 1000, 5 + oneWeek)
      ).toDF(colNames: _*).createOrReplaceTempView("events")

      val model = Seq("FIRST", "LAST", "LINEAR", "POSITION", "DECAY")
      val contrib = Seq(
        "[1.0];[0.0];[0.0];[0.0]"
        , "[0.0];[0.0];[0.0];[1.0]"
        , "[0.25];[0.25];[0.25];[0.25]"
        , "[0.4];[0.1];[0.1];[0.4]"
        , "[0.5];[0.5];[0.5];[1.0]"
      )

      model.zip(contrib).foreach { case (model, contrib) =>
        val df = spark.sql(
          "select explode(at) as event from (select attribution(604810000, ts, " +
            buildSource(Seq(1, 2, 3, 4)) +
            buildTarget(Seq(5)) +
            "null," +
            "'NONE', " +
            "null," +
            "array(measure1, measure2), " +
            s"'$model', " +
            "null) as at " +
            "from events group by uid) order by event.name"
        )
        val actual = df.selectExpr("event.contrib").collect().mkString(";")
        assert(contrib == actual)
      }
    }

    // test measures, grouping infos
    {
      Seq(
        (1, 1, "foo", "bar", 0, 0, 1),
        (1, 2, "foo1", "bar1", 0, 0, 2),
        (1, 3, "foo2", "bar2", 100, 1000, 3)
      ).toDF(colNames: _*).createOrReplaceTempView("events")

      val df = spark.sql(
        "select explode(at) as event from (select attribution(604810000, ts, " +
          buildSource(Seq(1, 2)) +
          buildTarget(Seq(3)) +
          "null," +
          "'NONE', " +
          "null," +
          "array(measure1, measure2), " +
          s"'LINEAR', " +
          "array(dim1, dim2)) as at " +
          "from events group by uid) order by event.name"
      )
      assert("[WrappedArray(50.0, 500.0)];[WrappedArray(50.0, 500.0)]" ==
        df.selectExpr("event.measureContrib").collect().mkString(";"))
      assert("[WrappedArray(foo, bar)];[WrappedArray(foo1, bar1)]" ==
        df.selectExpr("event.groupingInfos").collect().mkString(";"))
    }

    // test source relate to target, relation build
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 1),
        (1, 2, "bar", null, 0, 0, 5),
        (1, 3, "bar", null, 0, 0, 6),
        (1, 4, null, "foo", 0, 0, 7),
        (1, 5, "bar", null, 100, 0, 8),
        (1, 4, null, "bar", 0, 0, 9),
        (1, 5, null, "foo", 1000, 0, 10)
      ).toDF(colNames: _*).createOrReplaceTempView("events")

      val sql = "select explode(at) as event from (select attribution(7, ts, " +
        buildSource(Seq(1, 2, 3, 4)) +
        buildTarget(Seq(5)) +
        "null," +
        "'TARGET_TO_SOURCE', " +
        s"array(${buildMap(Seq("t5"), Seq("s2", "s3"), "dim1", "dim1")}," +
        s"${buildMap(Seq("t5"), Seq("s1", "s4"), "dim2", "dim2")})," +
        "array(measure1), " +
        s"'LINEAR', " +
        "null) as at " +
        "from events group by uid) order by event.name"

      val result = spark.sql(sql)
        .selectExpr("event.name", "event.measureContrib").collect().mkString(";")
      assert("[s2,WrappedArray(50.0)];[s3,WrappedArray(50.0)];[s4,WrappedArray(1000.0)]" == result)
    }

    // test ahead
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 4),
        (1, 2, "bar", null, 0, 0, 5),
        (1, 0, "bar", "foo", 0, 0, 6),
        (1, 3, "bar", null, 0, 0, 7),
        (1, 4, null, null, 0, 0, 8),
        (1, 0, "bar", "foo", 0, 0, 9),
        (1, 4, null, null, 0, 0, 10),
        (1, 5, null, "foo", 100, 0, 11)
      ).toDF(colNames: _*).createOrReplaceTempView("events")

      // ahead relate to target
      {
        val sql = "select explode(at) as event from (select attribution(7, ts, " +
          buildSource(Seq(1, 2, 3, 4)) +
          buildTarget(Seq(5)) +
          buildAhead(Seq(0)) +
          "'TARGET_TO_AHEAD', " +
          s"array(${buildMap(Seq("t5"), Seq("a0"), "dim2", "dim2")})," +
          "array(measure1), " +
          s"'LINEAR', " +
          "null) as at " +
          "from events group by uid) order by event.name"
        val result = spark.sql(sql)
          .selectExpr("event.name", "event.measureContrib").collect().mkString(";")
        assert("[s2,WrappedArray(50.0)];[s4,WrappedArray(50.0)]" == result)
      }

      // ahead relate to source and target
      {
        val sql = "select explode(at) as event from (select attribution(7, ts, " +
          buildSource(Seq(1, 2, 3, 4)) +
          buildTarget(Seq(5)) +
          buildAhead(Seq(0)) +
          "'TARGET_TO_AHEAD_TO_SOURCE', " +
          s"array(${buildMap(Seq("t5"), Seq("a0"), "dim2", "dim2")}," +
          s"${buildMap(Seq("a0"), Seq("s1", "s2", "s3", "s4"), "dim1", "dim1")})," +
          "array(measure1), " +
          s"'LINEAR', " +
          "null) as at " +
          "from events group by uid) order by event.name"
        val result = spark.sql(sql)
          .selectExpr("event.name", "event.measureContrib").collect().mkString(";")
        assert("[s2,WrappedArray(50.0)];[s3,WrappedArray(50.0)]" == result)
      }
    }

    // mutiple seq
    {
      Seq(
        (1, 1, null, "foo", 0, 0, 1),
        (1, 0, "bar", "foo1", 0, 0, 2), // aheah foo1
        (1, 1, null, "foo", 0, 0, 3),
        (1, 0, "bar", "foo1", 0, 0, 4), // aheah foo1
        (1, 2, "bar", null, 0, 0, 5),
        (1, 0, "bar", "foo", 0, 0, 6), // aheah foo
        (1, 3, "bar", null, 0, 0, 7),
        (1, 4, null, null, 0, 0, 8),
        (1, 0, "bar", "foo", 0, 0, 9), // aheah foo
        (1, 4, null, null, 0, 0, 10),
        (1, 5, null, "foo", 100, 0, 11),
        (1, 5, null, "foo1", 1000, 0, 12)
      ).toDF(colNames: _*).createOrReplaceTempView("events")

      val sql = "select explode(at) as event from (select attribution(9, ts, " +
        buildSource(Seq(1, 2, 3, 4)) +
        buildTarget(Seq(5)) +
        buildAhead(Seq(0)) +
        "'TARGET_TO_AHEAD', " +
        s"array(${buildMap(Seq("t5"), Seq("a0"), "dim2", "dim2")})," +
        "array(measure1), " +
        s"'LINEAR', " +
        "null) as at " +
        "from events group by uid) order by event.name"
      val result = spark
        .sql(sql).selectExpr("event.name", "event.measureContrib").collect().mkString(";")
      assert("[s1,WrappedArray(1000.0)];[s2,WrappedArray(50.0)];[s4,WrappedArray(50.0)]" == result)
    }
  }

  private def buildSource(eid: Seq[Int], comma: Boolean = true): String = {
    buildCaseWhen(eid, "s", comma)
  }

  private def buildTarget(eid: Seq[Int], comma: Boolean = true): String = {
    buildCaseWhen(eid, "t", comma)
  }

  private def buildAhead(eid: Seq[Int], comma: Boolean = true): String = {
    buildCaseWhen(eid, "a", comma)
  }

  private def buildCaseWhen(eid: Seq[Int], prefix: String, comma: Boolean = true): String = {
    val casewhen = "case " +
      eid.map { i => s"when eid=$i then '$prefix$i'"}.mkString(" ") +
      "else null end "
    if (comma) {
      s"$casewhen ,"
    } else {
      casewhen
    }
  }

  private def buildMap(e1List: Seq[String], e2List: Seq[String],
                       dim1: String, dim2: String): String = {
    (for (e1 <- e1List; e2 <- e2List) yield (e1, e2)).map { case (e1, e2) =>
      s"map('$e1', $dim1, '$e2', $dim2)"
    }.mkString(",")
  }

}

object TypedImperativeAggregateSuite {

  /**
   * Calculate the max value with object aggregation buffer. This stores class MaxValue
   * in aggregation buffer.
   */
  private case class TypedMax(
      child: Expression,
      nullable: Boolean = false,
      mutableAggBufferOffset: Int = 0,
      inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[MaxValue] with ImplicitCastInputTypes {


    override def createAggregationBuffer(): MaxValue = {
      // Returns Int.MinValue if all inputs are null
      new MaxValue(Int.MinValue)
    }

    override def update(buffer: MaxValue, input: InternalRow): MaxValue = {
      child.eval(input) match {
        case inputValue: Int =>
          if (inputValue > buffer.value) {
            buffer.value = inputValue
            buffer.isValueSet = true
          }
        case null => // skip
      }
      buffer
    }

    override def merge(bufferMax: MaxValue, inputMax: MaxValue): MaxValue = {
      if (inputMax.value > bufferMax.value) {
        bufferMax.value = inputMax.value
        bufferMax.isValueSet = bufferMax.isValueSet || inputMax.isValueSet
      }
      bufferMax
    }

    override def eval(bufferMax: MaxValue): Any = {
      if (nullable && bufferMax.isValueSet == false) {
        null
      } else {
        bufferMax.value
      }
    }

    override lazy val deterministic: Boolean = true

    override def children: Seq[Expression] = Seq(child)

    override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

    override def dataType: DataType = IntegerType

    override def withNewMutableAggBufferOffset(newOffset: Int): TypedImperativeAggregate[MaxValue] =
      copy(mutableAggBufferOffset = newOffset)

    override def withNewInputAggBufferOffset(newOffset: Int): TypedImperativeAggregate[MaxValue] =
      copy(inputAggBufferOffset = newOffset)

    override def serialize(buffer: MaxValue): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val stream = new DataOutputStream(out)
      stream.writeBoolean(buffer.isValueSet)
      stream.writeInt(buffer.value)
      out.toByteArray
    }

    override def deserialize(storageFormat: Array[Byte]): MaxValue = {
      val in = new ByteArrayInputStream(storageFormat)
      val stream = new DataInputStream(in)
      val isValueSet = stream.readBoolean()
      val value = stream.readInt()
      new MaxValue(value, isValueSet)
    }
  }

  private class MaxValue(var value: Int, var isValueSet: Boolean = false)
}
