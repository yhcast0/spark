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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateArray, CreateNamedStruct, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.UTF8StringBuilder
import org.apache.spark.unsafe.types.UTF8String


/**
 * @param windowLit window size in long
 * @param evtNumExpr number of events
 * @param eventTsCol event ts in long
 * @param evtConds expr to return event id (starting from 0)
 * @param dimValueExpr expr to return related dim values
 * @param stepIdPropsArray expr to return stepId attach props array
 */
case class WindowFunnel(windowLit: Expression,
                             evtNumExpr: Expression,
                             eventTsCol: Expression,
                             evtConds: Expression,
                             dimValueExpr: Expression,
                             stepIdPropsArray: Expression,
                             mutableAggBufferOffset: Int = 0,
                             inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Seq[Event]] with Serializable with Logging {

  def this(windowLit: Expression,
           evtNumExpr: Expression,
           eventTsCol: Expression,
           evtConds: Expression,
           dimValueExpr: Expression,
           stepIdPropsArray: Expression) = {
    this(windowLit, evtNumExpr, eventTsCol, evtConds,
      dimValueExpr, stepIdPropsArray, 0, 0)
  }

  lazy val window: Long = windowLit.eval().toString.toLong
  lazy val evtNum: Int = evtNumExpr.eval().toString.toInt

  override def createAggregationBuffer(): Seq[Event] = Seq[Event]()

  def toIntegerArray(expr: Expression, input: InternalRow): Array[Int] = {
    expr.dataType match {
      case _: NullType =>
        Array(-1)
      case _ =>
        // timezone doesn't really matter here
        expr.eval(input).toString.split(",").map(_.trim.toInt)
    }
  }

  def toLong(expr: Expression, input: InternalRow): Long = {
    expr.dataType match {
      case _: NumericType =>
        expr.eval(input).toString.toLong
      case _: TimestampType =>
        expr.eval(input).toString.toLong / 1000000
      case _: NullType =>
        -1L
      case _ =>
        // timezone doesn't really matter here
        val tsColumn = Cast(Cast(expr, TimestampType, Some("UTC")), LongType).eval(input)
        if (tsColumn == null) {
          return expr.eval(input).toString.toLong
        }
        tsColumn.toString.toLong
    }
  }

  def toString(expr: Expression, input: InternalRow): String = {
    val raw = expr.eval(input)
    if (raw != null) {
      raw.toString
    } else {
      null
    }
  }

  val attachProps = stepIdPropsArray.children
    .filter(e => e.isInstanceOf[CreateNamedStruct])
  val attachPropsIndexMap = {
    val attachPropsIndexMap = new ConcurrentHashMap[Expression, Integer]()
    for (i <- 0 until attachProps.length) {
      attachPropsIndexMap.put(attachProps.apply(i), i)
    }
    attachPropsIndexMap
  }
  val attachPropNum = attachProps.length
  val cacheEvalStepIdPropsArrayExpressionMap = {
    val expressionMap = new ConcurrentHashMap[Integer, CreateArray]()
    if (stepIdPropsArray == null) {
      expressionMap
    } else {
      var srcExpressionMap = new ConcurrentHashMap[Integer, util.ArrayList[Expression]]()
      attachProps.foreach(expression => {
        val stepId = expression.asInstanceOf[CreateNamedStruct].valExprs.apply(0).toString.toInt
        if (srcExpressionMap.get(stepId) == null) {
          srcExpressionMap.put(stepId, new util.ArrayList[Expression])
        }
        srcExpressionMap.get(stepId).add(expression)
      })
      srcExpressionMap.forEach((stepId, expressionSet) => {
        val array = new Array[Expression](expressionSet.size())
        var index = 0
        expressionSet.toArray.foreach(expression => {
          array(index) = expression.asInstanceOf[Expression]
          index = index + 1
        })
        val createArray = new CreateArray(array.toSeq)
        expressionMap.put(stepId, createArray)
      })
      srcExpressionMap = null
      expressionMap
    }
  }

  override def update(buffer: Seq[Event], input: InternalRow): Seq[Event] = {
    val evtIdArray = toIntegerArray(evtConds, input)
    val ts = toLong(eventTsCol, input)
    if (ts < 0) {
      return buffer
    }
    val dimValue = toString(dimValueExpr, input)
    val evtIdArrayMap = new ConcurrentHashMap[Integer, Array[Any]]()
    var hasAttach = false
    evtIdArray.foreach(evtId => {
      if (evtIdArray.apply(0) < 0 || evtIdArray.apply(0) >= evtNum) {
        return buffer
      }
      val stepIdAttachPropsArrayExpression =
        cacheEvalStepIdPropsArrayExpressionMap.get(evtId)
      if (stepIdAttachPropsArrayExpression != null) {
        hasAttach = true
        val attachPropsNameArray = stepIdAttachPropsArrayExpression
          .eval(input).asInstanceOf[GenericArrayData]
        val attachPropsArray = new Array[Any](attachPropsNameArray.array.length)
        var index = 0
        attachPropsNameArray.array.foreach(e => {
          val attachPropValue = e.asInstanceOf[GenericInternalRow].values.apply(1)
          if (attachPropValue.isInstanceOf[UTF8String]) {
            val strBuilder = new UTF8StringBuilder
            strBuilder.append("" + attachPropValue)
            attachPropsArray(index) = strBuilder.build()
          } else {
            attachPropsArray(index) = attachPropValue
          }
          index = index + 1
        })
        evtIdArrayMap.put(evtId, attachPropsArray)
      }
    })
    val event = Event(evtIdArray, ts, dimValue, evtIdArrayMap, evtIdArray.apply(0))
    buffer :+ event

  }

  override def merge(buffer: Seq[Event],
                     input: Seq[Event]): Seq[Event] = {
    buffer ++ input
  }

  val offsetMap = new ConcurrentHashMap[String, Integer]()
  override def eval(buffer: Seq[Event]): Any = {
    if (buffer.length == 0) {
      val returnRow = new GenericInternalRow(2 + attachPropNum)
      returnRow(0) = -1
      returnRow(1) = -1L
      return returnRow
    }

    val newBuffer = buffer.sortBy(e => e.ts).map(e => {
      if (e.eids.length > 1) {
        val eidKey = e.eids.mkString("_")
        val offset = offsetMap.get(eidKey)
        if (offset == null) {
          offsetMap.put(eidKey, 0)
          e
        } else {
          val newOffset = offset + 1
          if (newOffset + 1 <= e.eids.length) {
            offsetMap.put(eidKey, newOffset)
            Event(e.eids, e.ts, e.dim, e.attachPropsArrayMap, e.eids.apply(newOffset))
          } else {
            Event(e.eids, e.ts, e.dim, e.attachPropsArrayMap, -1)
          }
        }
      } else {
        e
      }
    })

    val grouped = newBuffer.groupBy(e => e.dim)
    val nullDimEvents = grouped.getOrElse(null, Seq())

    val result = grouped.map(e => {
      if (e._1 == null) {
        e
      } else {
        // append null dim events to other dims as they can concat to any sequences
        (e._1, e._2 ++ nullDimEvents)
      }
    }).map(e =>
      longestSeqId(e._2)
    )
    val orderResult = result.toSeq.sortBy(e => (e._1, e._2)).reverse
    val maxDepId = orderResult.apply(0)
    val returnRow = new GenericInternalRow(2 + attachPropNum)
    returnRow(0) = maxDepId._1
    returnRow(1) = maxDepId._2
    val attachPropValue = maxDepId._3
    if (attachPropValue != null) {
      for (i <- 0 until attachPropNum) {
        returnRow(i + 2) = attachPropValue.apply(i)
      }
    }
    returnRow

  }

  private def transformDataType(dataType: DataType): String = {
    dataType match {
      case BooleanType => "boolean"
      case ByteType => "tinyint"
      case ShortType => "smallint"
      case IntegerType => "integer"
      case LongType => "long"
      case FloatType => "float"
      case DoubleType => "double"
      case DecimalType.USER_DEFAULT => "decimal"
      case DateType => "date"
      case TimestampType => "timestamp"
      case BinaryType => "binary"
      case _ => "string"
    }
  }


  private def longestSeqId(events: Seq[Event]): (Int, Long, Array[Any]) = {

    var returnData = (-1, -1L, new Array[Any](attachPropNum))
    var maxStepId = -1
    if (evtNum < 1) {
      return returnData
    }
    val attachCurrentPropValuesMap = new util.HashMap[Long, Array[Any]]()
    val sorted = events.sortBy(e => (e.ts, e.eids.apply(0)))
    val timestamps = Array.fill[Long](evtNum)(-1)
    var lastId = -1
    sorted.filter(_.eid > -1).foreach(e => {
      val eid = e.eid
      if (eid == 0) {
        val currAttachPropsValues = new Array[Any](attachPropNum)
        val cacheAttachProps = cacheEvalStepIdPropsArrayExpressionMap.get(eid)
        if (cacheAttachProps != null) {
          assignAttachPropValue(cacheAttachProps, currAttachPropsValues, e, eid)
        }
        if (timestamps(eid) == -1) {
          returnData = (eid, e.ts, currAttachPropsValues)
        }
        timestamps(eid) = e.ts
        attachCurrentPropValuesMap.put(timestamps(eid), currAttachPropsValues)
      } else if (timestamps(eid -1) > -1 && timestamps(eid -1) + window >= e.ts) {
        val cacheAttachProps = cacheEvalStepIdPropsArrayExpressionMap.get(eid)
        if (lastId != eid
          && cacheAttachProps != null) {
          assignAttachPropValue(cacheAttachProps,
            attachCurrentPropValuesMap.get(timestamps(eid -1)), e, eid)
        }
        lastId = eid
        if (timestamps(eid) == -1) {
          returnData = (eid, timestamps(eid - 1),
            attachCurrentPropValuesMap.get(timestamps(0)))
        }
        timestamps(eid) = timestamps(eid - 1)
      }

      if (timestamps.last > -1) {
        maxStepId = timestamps.length - 1
        return returnData
      }
    })

    maxStepId = timestamps.lastIndexWhere(ts => ts > -1)
    val eventStartTime = attachCurrentPropValuesMap.get(returnData._2)
    if (eventStartTime == null) {
      return (-1, -1, null)
    }
    (maxStepId, returnData._2, attachCurrentPropValuesMap.get(returnData._2))

  }

  def assignAttachPropValue(cacheAttachProps: Expression,
                            currAttachPropsValues: Array[Any], e: Event, eid: Int): Unit = {
    for (i <- 0 until cacheAttachProps.children.length) {
      val curExpression = cacheAttachProps.children.apply(i)
      val attachPropIndex = attachPropsIndexMap.get(curExpression)
      if (currAttachPropsValues(attachPropIndex) == null) {
        currAttachPropsValues(attachPropIndex) = e.attachPropsArrayMap.get(eid).apply(i)
      } else {
        for (j <- 0 until attachProps.length) {
          if (curExpression == attachProps.apply(j)) {
            currAttachPropsValues(j) = e.attachPropsArrayMap.get(eid).apply(i)
          }
        }
      }
    }
  }

  override def serialize(buffer: Seq[Event]): Array[Byte] = {
    SerializeEqualUtil.serialize(buffer)
  }

  override def deserialize(storageFormat: Array[Byte]): Seq[Event] = {
    SerializeEqualUtil.deserialize(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  val field_max_stepId = "{\"name\":\"field_max_stepId\",\"type\":" +
    "\"integer\",\"nullable\":false,\"metadata\":{}}"
  val field_start_time = "{\"name\":\"field_start_time\",\"type\":" +
    "\"long\",\"nullable\":false,\"metadata\":{}}"

  override def dataType: DataType = {
    var fields = field_max_stepId + "," + field_start_time
    for (i <- 0 until attachPropNum) {
      val children = attachProps.apply(i)
      val nameExprs = children.asInstanceOf[CreateNamedStruct].nameExprs
      val propertyColumn = nameExprs.apply(1)
      val propertyColumnName = propertyColumn.asInstanceOf[Literal].value
      val propertyColumnType = children.dataType.asInstanceOf[StructType].apply(1).dataType
      fields = fields + ",{\"name\":\"" + propertyColumnName +
        "\",\"type\":\"" + transformDataType(propertyColumnType) +
        "\",\"nullable\":true,\"metadata\":{}}"
    }
    DataType.fromJson("{\"type\":\"struct\",\"fields\":[" +
      fields +
      "]}")
  }

  override def children: Seq[Expression] =
    windowLit :: eventTsCol :: evtNumExpr :: evtConds :: dimValueExpr :: stepIdPropsArray :: Nil
}

case class Event(eids: Array[Int], ts: Long, dim: String,
                      attachPropsArrayMap: ConcurrentHashMap[Integer, Array[Any]], eid: Int)

object SerializeEqualUtil {
  import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

  def writeEvent(oss: ObjectOutputStream, event: Event): Unit = {
    oss.writeObject(event.eids)
    oss.writeLong(event.ts)
    if (event.dim == null) {
      oss.writeBoolean(true)
    } else {
      oss.writeBoolean(false)
      oss.writeUTF(event.dim)
    }
    oss.writeObject(event.attachPropsArrayMap)
    oss.writeInt(event.eid)
  }

  def readEvent(ois: ObjectInputStream): Event = {
    val id = ois.readObject().asInstanceOf[Array[Int]]
    val ts = ois.readLong()
    if (ois.readBoolean()) {
      Event(id, ts, null, ois.readObject()
        .asInstanceOf[ConcurrentHashMap[Integer, Array[Any]]], ois.readInt())
    } else {
      Event(id, ts, ois.readUTF(), ois.readObject()
        .asInstanceOf[ConcurrentHashMap[Integer, Array[Any]]], ois.readInt())
    }
  }

  def serialize(events: Seq[Event]): Array[Byte] = {
    try {
      val baos = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(baos)
      oos.writeInt(events.length)
      events.foreach(e => writeEvent(oos, e))
      oos.flush()
      val bytes = baos.toByteArray
      bytes
    } catch {
      case e: Exception =>
        throw new RuntimeException("serialize failed", e)
    }
  }

  def deserialize(bytes: Array[Byte]): Seq[Event] = {
    try {
      val bais = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bais)
      val size = ois.readInt()
      (0 until size).map(_ => readEvent(ois))
    } catch {
      case e: Exception =>
        throw new RuntimeException("deserialize failed", e)
    }
  }
}
