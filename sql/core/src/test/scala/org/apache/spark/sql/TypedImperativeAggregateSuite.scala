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
import org.apache.spark.sql.catalyst.expressions.aggregate.{TypedImperativeAggregate, WindowFunnel}
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


  private def checkWindowAnswer (df: DataFrame, expected: Seq[(Int, Int)] ): Unit = {
    df.show (false)
    checkAnswer (df, expected.toDF () )
  }

  ignore ("test window funnel 0004") {
    val colNames = Seq ("event_id", "user_id", "uid", "event_time", "access_time",
      "string1", "string2", "int1", "int2",
      "bigint1", "bigint2", "double1", "double2", "id")
    val df1 = Seq (
      (0, 100000001, "200000001", "1647581938058", "1647581638051",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 6)
      , (1, 100000001, "200000002", "1647581938059", "1647581638052",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 7)
      , (0, 100000001, "200000003", "1647581938060", "1647581638053",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 8)
      , (1, 100000001, "200000004", "1647581938061", "1647581638054",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 9)
      , (0, 100000001, "200000005", "1647581938062", "1647581638055",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 10)
      , (1, 100000001, "200000006", "1647581938063", "1647581638056",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 11)
      , (1, 100000001, "200000007", "1647581938064", "1647581638057",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 12)
      , (2, 100000001, "200000008", "1647581938065", "1647581638058",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 13)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events1")
    checkWindowAnswer (spark.sql (
      "select user_id, window_funnel(\n" +
        " 5 * 24 * 60 * 60,\n" +
        " 6,\n" +
        " event_time,\n" +
        " case when event_id = 0 then '0, 2'\n" +
        " when event_id = 1 then '1,3'\n" +
        " when event_id = 2 then '4'\n" +
        " else '-1' end,\n" +
        " bigint1,\n" +
        " struct(struct(2, user_id), struct(1, id), struct(3, id)) ) seq\n" +
        "from events1\n" +
        "where id >=6 and id <=35\n" +
        "group by user_id\n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 0003") {
    val colNames = Seq ("event_id", "user_id", "uid", "event_time", "access_time",
      "string1", "string2", "int1", "int2",
      "bigint1", "bigint2", "double1", "double2", "id")
    val df1 = Seq (
      (0, 100000001, "200000001", "1647581938058", "1747581638051",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 6)
      , (1, 100000001, "200000002", "1647581938059", "1747581638052",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 7)
      , (2, 100000001, "200000003", "1647581938060", "1747581638053",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 8)
      , (0, 100000001, "200000004", "1647581938061", "1747581638054",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 9)
      , (1, 100000001, "200000005", "1647581938062", "1747581638055",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 10)
      , (2, 100000001, "200000006", "1647581938063", "1747581638056",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 11)
      , (3, 100000001, "200000007", "1647581938064", "1747581638057",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 12)
      , (4, 100000001, "200000008", "1647581938065", "1747581638058",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 13)
      , (0, 100000001, "200000009", "1647581938066", "1747581638059",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 14)
      , (1, 100000001, "200000010", "1647581938067", "1747581638060",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 15)
      , (2, 100000001, "200000011", "1647581938068", "1747581638061",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 16)
      , (3, 100000001, "200000012", "1647581938069", "1747581638062",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 17)
      , (0, 100000002, "200000013", "1647581938070", "1747581638063",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 18)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events1")
    checkWindowAnswer (spark.sql (
      "select user_id, window_funnel(\n" +
        " 5 * 24 * 60 * 60,\n" +
        " 6,\n" +
        " event_time,\n" +
        " case when event_id = 0 then '0'\n" +
        " when event_id = 1 then '1'\n" +
        " when event_id = 2 then '2'\n" +
        " when event_id = 3 then '3'\n" +
        " when event_id = 4 then '4'\n" +
        " else -1 end,\n" +
        " bigint1,\n" +
        " struct(struct(2, user_id), struct(1, id), struct(3, id)) ) seq\n" +
        "from events1\n" +
        "where id >=6 and id <=35\n" +
        "group by user_id\n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 0002") {
    val colNames = Seq ("event_id", "user_id", "uid", "event_time", "access_time",
      "string1", "string2", "int1", "int2",
      "bigint1", "bigint2", "double1", "double2", "id")
    val df1 = Seq (
      (0, 100000001, "200000001", "1647581938058", "1647581638051",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 6)
      , (1, 100000001, "200000002", "1647581938059", "1647581638052",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 7)
      , (2, 100000001, "200000003", "1647581938060", "1647581638053",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 8)
      , (3, 100000001, "200000004", "1647581938061", "1647581638054",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 9)
      , (4, 100000001, "200000005", "1647581938062", "1647581638055",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 10)
      , (5, 100000001, "200000006", "1647581938063", "1647581638056",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 11)
      , (0, 100000002, "200000007", "1647581938064", "1647581638057",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 12)
      , (1, 100000002, "200000008", "1647581938065", "1647581638058",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 13)
      , (2, 100000002, "200000009", "1647581938066", "1647581638059",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 14)
      , (3, 100000002, "200000010", "1647581938067", "1647581638060",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 15)
      , (4, 100000002, "200000011", "1647581938068", "1647581638061",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 16)
      , (5, 100000002, "200000012", "1647581938069", "1647581638062",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 17)
      , (0, 100000003, "200000013", "1647581938070", "1647581638063",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 18)
      , (1, 100000003, "200000014", "1647581938071", "1647581638064",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 19)
      , (2, 100000003, "200000015", "1647581938072", "1647581638065",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 20)
      , (3, 100000003, "200000016", "1647581938073", "1647581638066",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 21)
      , (4, 100000003, "200000017", "1647581938074", "1647581638067",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 22)
      , (5, 100000003, "200000018", "1647581938075", "1647581638068",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 23)
      , (0, 100000004, "200000019", "1647581938076", "1647581638069",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 24)
      , (1, 100000004, "200000020", "1647581938077", "1647581638070",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 25)
      , (2, 100000004, "200000021", "1647581938078", "1647581638071",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 26)
      , (3, 100000004, "200000022", "1647581938079", "1647581638072",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 27)
      , (4, 100000004, "200000023", "1647581938080", "1647581638073",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 28)
      , (5, 100000004, "200000024", "1647581938081", "1647581638074",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 29)
      , (0, 100000005, "200000025", "1647581938082", "1647581638075",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 30)
      , (1, 100000005, "200000026", "1647581938083", "1647581638076",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 31)
      , (2, 100000005, "200000027", "1647581938084", "1647581638077",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 32)
      , (3, 100000005, "200000028", "1647581938085", "1647581638078",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 33)
      , (4, 100000005, "200000029", "1647581938086", "1647581638079",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 34)
      , (5, 100000005, "200000030", "1647581938087", "1647581638080",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 35)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events1")
    checkWindowAnswer (spark.sql (
      "select user_id, window_funnel(\n" +
        " 5 * 24 * 60 * 60,\n" +
        " 6,\n" +
        " event_time,\n" +
        " case when event_id = 0 or event_id = 2 then '0, 2'\n" +
        " when event_id = 1 then '1'\n" +
        " when event_id = 3 then '3'\n" +
        " else -1 end,\n" +
        " bigint1,\n" +
        " struct(struct(2, user_id), struct(1, id), struct(3, id)) ) seq\n" +
        "from events1\n" +
        "where id >=6 and id <=35\n" +
        "group by user_id\n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 0001") {
    val colNames = Seq ("event_id", "user_id", "uid", "event_time", "access_time",
      "string1", "string2", "int1", "int2",
      "bigint1", "bigint2", "double1", "double2", "id")
    val df1 = Seq (
      (0, 100000001, "200000001", "1647581938058", "1647581638051",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 6)
      , (1, 100000001, "200000002", "1647581938059", "1647581638052",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 7)
      , (0, 100000001, "200000003", "1647581938060", "1647581638053",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 8)
      , (3, 100000001, "200000004", "1647581938061", "1647581638054",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 9)
      , (4, 100000001, "200000005", "1647581938062", "1647581638055",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 10)
      , (5, 100000001, "200000006", "1647581938063", "1647581638056",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 11)
      , (0, 100000002, "200000007", "1647581938064", "1647581638057",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 12)
      , (1, 100000002, "200000008", "1647581938065", "1647581638058",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 13)
      , (0, 100000002, "200000009", "1647581938066", "1647581638059",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 14)
      , (3, 100000002, "200000010", "1647581938067", "1647581638060",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 15)
      , (4, 100000002, "200000011", "1647581938068", "1647581638061",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 16)
      , (5, 100000002, "200000012", "1647581938069", "1647581638062",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 17)
      , (0, 100000003, "200000013", "1647581938070", "1647581638063",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 18)
      , (1, 100000003, "200000014", "1647581938071", "1647581638064",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 19)
      , (0, 100000003, "200000015", "1647581938072", "1647581638065",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 20)
      , (3, 100000003, "200000016", "1647581938073", "1647581638066",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 21)
      , (4, 100000003, "200000017", "1647581938074", "1647581638067",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 22)
      , (5, 100000003, "200000018", "1647581938075", "1647581638068",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 23)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events1")
    checkWindowAnswer (spark.sql (
      "select user_id, window_funnel(\n" +
        " 5 * 24 * 60 * 60,\n" +
        " 6,\n" +
        " event_time,\n" +
        // " case when event_id = 0 or event_id = 2 then '0, 2'\n" +
        " case when event_id = 0 then '0, 2'\n" +
        " when event_id = 1 then '1'\n" +
        // " when event_id = 2 then '2'\n" +
        " when event_id = 3 then '3'\n" +
        // " when event_id = 4 then '4'\n" +
        // " when event_id = 5 then '5'\n" +
        " else -1 end, \n" +
        // " cast(event_id as char),\n" +
        // " event_id,\n" +
        // " array(0, 1, 0),\n" +
        " bigint1,\n" +
        " struct(struct(2, user_id), struct(1, id), struct(3, id))\n ) seq\n" +
        "from events1\n" +
        "group by user_id\n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 0000") {
    val colNames = Seq ("event_id", "user_id", "uid", "event_time", "access_time",
      "string1", "string2", "int1", "int2",
      "bigint1", "bigint2", "double1", "double2", "id")
    val df1 = Seq (
      (0, 100000001, "200000001", "1647581938058", "1647581638051",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 6)
      , (1, 100000001, "200000002", "1647581938059", "1647581638052",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 7)
      , (2, 100000001, "200000003", "1647581938060", "1647581638053",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 8)
      , (3, 100000001, "200000004", "1647581938061", "1647581638054",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 9)
      , (4, 100000001, "200000005", "1647581938062", "1647581638055",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 10)
      , (5, 100000001, "200000006", "1647581938063", "1647581638056",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 11)
      , (0, 100000002, "200000007", "1647581938064", "1647581638057",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 12)
      , (1, 100000002, "200000008", "1647581938065", "1647581638058",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 13)
      , (2, 100000002, "200000009", "1647581938066", "1647581638059",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 14)
      , (3, 100000002, "200000010", "1647581938067", "1647581638060",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 15)
      , (4, 100000002, "200000011", "1647581938068", "1647581638061",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 16)
      , (5, 100000002, "200000012", "1647581938069", "1647581638062",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 17)
      , (0, 100000003, "200000013", "1647581938070", "1647581638063",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 18)
      , (1, 100000003, "200000014", "1647581938071", "1647581638064",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 19)
      , (2, 100000003, "200000015", "1647581938072", "1647581638065",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 20)
      , (3, 100000003, "200000016", "1647581938073", "1647581638066",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 21)
      , (4, 100000003, "200000017", "1647581938074", "1647581638067",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 22)
      , (5, 100000003, "200000018", "1647581938075", "1647581638068",
        "string_1", "string_2", 1, 2, 1L, 2L, 1d, 2d, 23)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events1")
    checkWindowAnswer (spark.sql (
      "select user_id, window_funnel(\n" +
        " 5 * 24 * 60 * 60,\n" +
        " 6,\n" +
        " event_time,\n" +
        " event_id,\n" +
        " bigint1,\n" +
        " struct(struct(2, user_id), struct(1, id))\n ) seq\n" +
        "from events1\n" +
        "group by user_id\n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 002") {
    val colNames = Seq ("lstg_format_name", "cal_dt", "lstg_site_id",
      "seller_id", "trans_id")

    val df1 = Seq (
      ("ABIN", "2012-01-01", 0, 10000294, 0)
      , ("FP-non GTC", "2012-01-01", 0, 10000294, 1)
      , ("Others", "2012-01-01", 0, 10000294, 2)
      , ("Auction", "2012-01-01", 0, 10000294, 3)
      , ("ABIN", "2012-01-01", 1, 10000294, 0)
      , ("FP-non GTC", "2012-01-01", 1, 10000294, 1)
      , ("Others", "2012-01-01", 1, 10000294, 2)

    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("test_kylin_fact")
    checkWindowAnswer (spark.sql (
      "select seller_id," +
        " window_funnel(\n 5 * 24 * 60 * 60, \n" +
        " 4, \n" +
        " cal_dt, \n" +
        " case when lstg_format_name = 'ABIN' or lstg_format_name = 'FP-non GTC' then '0,1'\n" +
        " when lstg_format_name = 'Others' then 2\n" +
        " when lstg_format_name = 'Auction' then 3\n" +
        " else -1 end, \n" +
        " case when lstg_format_name = 'ABIN' then lstg_site_id\n" +
        " when lstg_format_name = 'FP-non GTC' then lstg_site_id\n" +
        " when lstg_format_name = 'Others' then lstg_site_id\n" +
        " when lstg_format_name = 'Auction' then lstg_site_id\n" +
        " else null end,\n" +
        " struct(struct(1, seller_id), struct(3, trans_id)) \n ) seq\n" +
        "from test_kylin_fact \n\n" +
        "where cal_dt >= '2012-01-01' and cal_dt < '2012-01-02' \n" +
        "group by seller_id \n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 001") {
    val colNames = Seq ("lstg_format_name", "cal_dt", "lstg_site_id",
      "seller_id", "trans_id")

    val df1 = Seq (
      ("ABIN", "2012-01-01", 0, 10000294, 0)
      , ("FP-non GTC", "2012-01-01", 0, 10000294, 1)
      , ("Others", "2012-01-01", 0, 10000294, 2)
      , ("Auction", "2012-01-01", 0, 10000294, 3)
      , ("ABIN", "2012-01-01", 1, 10000294, 0)
      , ("FP-non GTC", "2012-01-01", 1, 10000294, 1)
      , ("Others", "2012-01-01", 1, 10000294, 2)
      , ("Auction", "2012-01-01", 0, 10000403, 4)
      , ("FP-non GTC", "2012-01-01", 15, 10000397, 5)
      , ("Auction", "2012-01-01", 0, 10000025, 6)
      , ("FP-GTC", "2012-01-01", 0, 10000670, 7)
      , ("FP-non GTC", "2012-01-01", 0, 10000288, 8)
      , ("FP-non GTC", "2012-01-01", 0, 10000753, 9)
      , ("Others", "2012-01-01", 0, 10000117, 10)
      , ("FP-non GTC", "2012-01-01", 0, 10000392, 11)
      , ("FP-non GTC", "2012-01-01", 0, 10000207, 12)
      , ("ABIN", "2012-01-01", 0, 10000896, 13)
      , ("ABIN", "2012-01-01", 23, 10000527, 14)
      , ("FP-GTC", "2012-01-01", 3, 10000549, 15)
      , ("ABIN", "2012-01-01", 3, 10000081, 16)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("test_kylin_fact")
    checkWindowAnswer (spark.sql (
      "select seller_id," +
        " window_funnel(\n 5 * 24 * 60 * 60, \n" +
        " 4, \n" +
        " cal_dt, \n" +
        " case when lstg_format_name = 'ABIN' then 0\n" +
        " when lstg_format_name = 'FP-non GTC' then 1\n" +
        " when lstg_format_name = 'Others' then 2\n" +
        " when lstg_format_name = 'Auction' then 3\n" +
        " else -1 end, \n" +
        " case when lstg_format_name = 'ABIN' then lstg_site_id\n" +
        " when lstg_format_name = 'FP-non GTC' then lstg_site_id\n" +
        " when lstg_format_name = 'Others' then lstg_site_id\n" +
        " when lstg_format_name = 'Auction' then lstg_site_id\n" +
        " else null end,\n" +
        " struct(struct(1, seller_id), struct(3, trans_id)) \n ) seq\n" +
        "from test_kylin_fact \n\n" +
        "where cal_dt >= '2012-01-01' and cal_dt < '2012-01-02' \n" +
        "group by seller_id \n" +
        "order by seq desc\n" +
        "LIMIT 500"
    ), Seq ((901, 5), (902, 2) ) )
  }

  ignore ("test window funnel 000") {
    val colNames = Seq ("uid", "eid", "dim1", "dim2",
      "o_dim1", "o_dim2", "o_dim3", "dim3", "dim4", "ts")

    val df1 = Seq (
      (901, 1, "null1", "aa1", 1L, 2.1d, 0.1f, null, null, "2021-01-02")
      , (901, 2, "null2", "aa2", 2L, 2.2d, 0.2f, null, null, "2021-01-02")
      , (901, 3, "null3", "aa3", 3L, 2.3d, 0.3f, null, null, "2021-01-03")
      , (901, 4, "null4", "aa4", 4L, 2.41d, 0.41f, null, null, "2021-01-03")
      , (901, 4, "null4", "aa4", 4L, 2.42d, 0.42f, null, null, "2021-01-04")
      , (901, 5, "null5", "aa5", 5L, 2.5d, 0.5f, null, null, "2021-01-05")
      , (901, 1, "w0001", "888", 11L, 3.1d, 0.6f, null, null, "2021-01-02")
      , (901, 2, "w0002", "bb7", 12L, 3.2d, 0.7f, null, null, "2021-01-03")
      , (901, 3, "w0003", "bb8", 13L, 3.3d, 0.8f, null, null, "2021-01-04")
      , (901, 6, "null6", "aa6", 6L, 2.6d, 0.6f, null, null, "2021-01-05")
      , (902, 1, "ee001", "cc1", 21L, 4.1d, 0.9f, null, null, "2021-01-02")
      , (902, 2, "ee002", "cc2", 22L, 4.2d, 0.11f, null, null, "2021-01-03")
      , (902, 3, "ee003", "cc3", 23L, 4.3d, 0.12f, null, null, "2021-01-03")
      , (902, 4, "ee004", "cc4", 24L, 4.4d, 0.13f, null, null, "2021-01-04")
      , (902, 5, "ee005", "cc5", 25L, 4.5d, 0.14f, null, null, "2021-01-05")
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events00")
    checkWindowAnswer (spark.sql (
      "select uid, " +
        "window_funnel(100000, 6, ts, eid - 1, dim3, " +
        "struct(struct(1,eid),struct(2,dim1),struct(1,eid)" +
        ",struct(2,dim2),struct(2,dim2)" +
        ",struct(3,o_dim1),struct(3,o_dim2),struct(3,o_dim3),struct(3,dim4)" +
        "))" +
        " from events00 group by uid"), Seq ((901, 5), (902, 2) ) )
  }

  // ignore("test window funnel") {
  ignore ("test window funnel") {
    val colNames = Seq ("uid", "eid", "dim1", "dim2",
      "o_dim1", "o_dim2", "o_dim3", "dim3", "ts")

    val df1 = Seq (
      // "uid", "eid", "dim1", "dim2", "dim3", "ts"
      // single sequence
      (901, 1, "null1", "aa1", 1L, 2.1d, 0.1f, null, 1646364222007L)
      , (901, 2, "null2", "aa2", 2L, 2.2d, 0.2f, null, 1646364236022L)
      , (901, 3, "null3", "aa3", 3L, 2.3d, 0.3f, null, 1646364251414L)
      , (901, 4, "null4", "aa4", 4L, 2.41d, 0.41f, null, 1646364263738L)
      , (901, 4, "null4", "aa4", 4L, 2.42d, 0.42f, null, 1646364263739L)
      , (901, 5, "null5", "aa5", 5L, 2.5d, 0.5f, null, 1646364273238L)
      , (901, 1, "w0001", "888", 11L, 3.1d, 0.6f, null, 1600001L)
      , (901, 2, "w0002", "bb7", 12L, 3.2d, 0.7f, null, 1600002L)
      , (901, 3, "w0003", "bb8", 13L, 3.3d, 0.8f, null, 1600003L)
      , (901, 6, "null6", "aa6", 6L, 2.6d, 0.6f, null, 1646364273998L)
      , (902, 1, "ee001", "cc1", 21L, 4.1d, 0.9f, null, 1001L)
      , (902, 2, "ee002", "cc2", 22L, 4.2d, 0.11f, null, 1002L)
      , (902, 3, "ee003", "cc3", 23L, 4.3d, 0.12f, null, 1003L)
      , (902, 4, "ee004", "cc4", 24L, 4.4d, 0.13f, null, 1004L)
      , (902, 5, "ee005", "cc5", 25L, 4.5d, 0.14f, null, 1005L)
    ).toDF (colNames: _*)
    df1.createOrReplaceTempView ("events00")
    checkWindowAnswer (spark.sql (
      "select uid, " +
        "window_funnel(100000, 6, ts, eid - 1, dim3, " +
        "struct(struct(1,eid),struct(2,dim1),struct(1,eid)" +
        ",struct(2,dim2),struct(2,dim2)" +
        ",struct(3,o_dim1),struct(3,o_dim2),struct(3,o_dim3)" +
        "))" +
        " from events00 group by uid"), Seq ((901, 5), (902, 2) ) )

    val df2 = Seq (
      // "uid", "eid", "dim1", "dim2", "ts"
      // multiple sequence
      (901, 1, null, null, 1)
      , (901, 2, null, null, 2)
      , (901, 1, null, null, 3)
      , (901, 3, null, null, 4)
      , (901, 1, null, null, 5)
      , (901, 2, null, null, 6)
      , (901, 4, null, null, 7)
      , (901, 5, null, null, 11)
    ).toDF (colNames: _*)
    df2.createOrReplaceTempView ("events")
    checkWindowAnswer (spark.sql ("select uid, window_funnel(10, 5, ts, eid - 1, null)" +
      " from events group by uid"), Seq ((901, 5) ) )

    val df3 = Seq (
      // "uid", "eid", "dim1", "dim2", "ts"
      // multiple sequence with dim
      (901, 1, "CN", null, 1)
      , (901, 2, null, null, 2)
      , (901, 1, null, null, 3)
      , (901, 3, null, "CN", 4)
      , (901, 1, null, null, 5)
      , (901, 2, null, null, 6)
      , (901, 4, null, "CN", 7)
      , (901, 5, null, null, 8)
      // multiple sequence with dim
      , (902, 1, "CN", null, 1)
      , (902, 2, null, null, 2)
      , (902, 1, null, null, 3)
      , (902, 3, null, "CN", 4)
      , (902, 1, null, null, 5)
      , (902, 2, null, null, 6)
      , (902, 4, null, "US", 7)
      , (902, 5, null, null, 8)
    ).toDF (colNames: _*)
    df3.createOrReplaceTempView ("events")
    checkWindowAnswer (spark.sql (
      "select uid, window_funnel(10, 5, ts, eid - 1, " +
        "case when eid = 1 then dim1 " +
        "when eid = 2 then null " +
        "when eid = 3 then dim2 " +
        "when eid = 4 then dim2 " +
        "else null end) from events group by uid"),
      Seq ((901, 5), (902, 3) ) )
  }

  ignore ("test window funnel radnom") {
    val events = (0 until 1000000).map {
      _ =>
        (random.nextInt (100000) + 1, // uid
          random.nextInt (5) + 1, // evt id 1-5
          "dim_" + random.nextInt (100), // dim col1 0-2
          "dim_" + random.nextInt (100), // dim col2 0-2
          random.nextInt (1000), // ts
        )
    }
    val df = events.toDF ("uid", "eid", "dim1", "dim2", "ts")
    // println(s"generated")

    //    df.orderBy("uid").show(100, false)

    val wf = WindowFunnel (
      lit (100).expr,
      lit (5).expr,
      Column ("ts").expr,
      Column ("eid").expr,
      lit (null).expr,
      null
    ).toAggregateExpression ()

    println (s"started")
    (0 until 5).foreach {
      _ =>
        val agg = df.groupBy ("uid").agg (Column (wf) )
        val started = System.currentTimeMillis ()
        val result = agg.collect ()
        assert (result != null)
        println (s">>>>> collected ${
          System.currentTimeMillis () - started
        }ms")
    }
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
