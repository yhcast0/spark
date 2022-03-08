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
import org.apache.spark.sql.catalyst.expressions.{Cast, CreateNamedStruct, Expression, GenericInternalRow, Literal, PrettyAttribute}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.UTF8StringBuilder


/**
 * @param windowLit window size in long
 * @param evtNumExpr number of events
 * @param eventTsCol event ts in long
 * @param evtConds expr to return event id (starting from 0)
 * @param dimValueExpr expr to return related dim values
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
    this(windowLit, evtNumExpr, eventTsCol, evtConds, dimValueExpr, stepIdPropsArray, 0, 0)
  }

  lazy val window: Long = windowLit.eval().toString.toLong
  lazy val evtNum: Int = evtNumExpr.eval().toString.toInt

  override def createAggregationBuffer(): Seq[Event] = Seq[Event]()

  def toBoolean(expr: Expression, input: InternalRow): Boolean = {
    expr.eval(input).toString.toBoolean
  }

  def toInteger(expr: Expression, input: InternalRow): Int = {
    expr.eval(input).toString.toInt
  }

  def toLong(expr: Expression, input: InternalRow): Long = {
    expr.dataType match {
      case _: NumericType =>
        expr.eval(input).toString.toLong
      case _: TimestampType =>
        expr.eval(input).toString.toLong / 1000000
      case _ =>
        // timezone doesn't really matter here
        Cast(Cast(expr, TimestampType, Some("UTC")), LongType).eval(input).toString.toLong
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

  val attachPropertiesNum = stepIdPropsArray.children.length
  val cacheStepIdWithPropMap = new ConcurrentHashMap[String, Any]()
  val attachPropMap = {
    val propMap = new ConcurrentHashMap[Integer, util.HashSet[String]]()
    for (i <- 0 until attachPropertiesNum) {
      val children = stepIdPropsArray.children.apply(i)
      val nameExprs = children.asInstanceOf[CreateNamedStruct].nameExprs
      val propertyColumn = nameExprs.apply(1)
      val stepId = children.asInstanceOf[CreateNamedStruct].valExprs.apply(0).toString.toInt
      if (propMap.get(stepId) == null) {
        propMap.put(stepId, new util.HashSet[String])
      }
      var propertyColumnName = ""
      if (propertyColumn.isInstanceOf[PrettyAttribute]) {
        propertyColumnName = propertyColumn.asInstanceOf[PrettyAttribute].name
      } else if (propertyColumn.isInstanceOf[Literal]) {
        propertyColumnName = propertyColumn.asInstanceOf[Literal].value + ""
      }
      propMap.get(stepId).add(propertyColumnName)
    }
    propMap
  }

  override def update(buffer: Seq[Event], input: InternalRow): Seq[Event] = {
    val evtId = toInteger(evtConds, input)
    if (evtId < 0 || evtId >= evtNum) {
      return buffer
    }
    val ts = toLong(eventTsCol, input)
    val dimValue = toString(dimValueExpr, input)
    if (stepIdPropsArray != null) {
      val attachPropsArray = stepIdPropsArray.eval(input).asInstanceOf[GenericArrayData]
      val attachPropArray = new Array[Any](attachPropsArray.array.length)
      var index = 0
      attachPropsArray.array.foreach(e => {
        val attachDataArray = e.asInstanceOf[GenericInternalRow].values
        val stepId = attachDataArray.apply(0)
        val attachPropValue = attachDataArray.apply(1)
        if (evtId == stepId) {
          attachPropArray(index) = attachPropValue.toString
          index = index + 1
        }
      })
      val event = Event(evtId, ts, dimValue, attachPropArray)
      buffer :+ event
    } else {
      val event = Event(evtId, ts, dimValue, null)
      buffer :+ event
    }
  }

  override def merge(buffer: Seq[Event],
                     input: Seq[Event]): Seq[Event] = {
    buffer ++ input
  }

  override def eval(buffer: Seq[Event]): Any = {
    if (buffer.length == 0) {
      val returnArray = new Array[Int](1)
      returnArray(0) = -1
      return returnArray
    }

    val grouped = buffer.groupBy(e => e.dim)
    val nullDimEvents = grouped.getOrElse(null, Seq())


    val result = grouped.map(e => {
      if (e._1 == null) {
        e
      } else {
        // append null dim events to other dims as they can concat to any sequences
        (e._1, e._2 ++ nullDimEvents)
      }
    }).map(e =>
      longestSeqId(e._2,
        cacheStepIdWithPropMap)
    )
    val orderResult = result.toSeq.sortBy(e => (e._1, e._2))
    val maxDepId = orderResult.apply(0)
    val returnRow = new GenericInternalRow(2 + attachPropertiesNum)
    returnRow(0) = maxDepId._1
    returnRow(1) = maxDepId._2

    for (i <- 0 until attachPropertiesNum) {
      val children = stepIdPropsArray.children.apply(i)
      val nameExprs = children.asInstanceOf[CreateNamedStruct].nameExprs
      val stepId = children.asInstanceOf[CreateNamedStruct].valExprs
        .apply(0).asInstanceOf[Literal].value
      val propertyColumn = nameExprs.apply(1)
      val propertyColumnName = propertyColumn.asInstanceOf[Literal].value
      val propertyColumnType = children.dataType.asInstanceOf[StructType].apply(i).dataType
      val attachPropValue = cacheStepIdWithPropMap
        .get(maxDepId._2 + "_" + stepId + "_" + propertyColumnName)
      returnRow(i + 2) = generateData(attachPropValue, propertyColumnType)
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

  private def generateData(data: Any, dataType: DataType): Any = {
    dataType match {
      case StringType =>
        val strBuilder = new UTF8StringBuilder
        strBuilder.append(data + "")
        strBuilder.build()
      case _ => data
    }
  }

  private def longestSeqId(events: Seq[Event],
        cacheStepIdWithPropMap: ConcurrentHashMap[String, Any]
      ): (Int, Long) = {

    val sorted = events.sortBy(e => (e.ts, e.eid))
    val startTimeStamp = sorted.apply(0).ts

    sorted.foreach(e => {
      val attachProps = attachPropMap.get(e.eid)
      if (attachProps != null) {
        val it = attachProps.iterator()
        var index = 0
        while (it.hasNext) {
          val attachPropColName = it.next()
          cacheStepIdWithPropMap.put(startTimeStamp + "_" + e.eid + "_" + attachPropColName,
            e.attachPropsArray(index))
          index = index + 1
        }
      }
    })

    var maxStepId = -1
    val timestamps = Array.fill[Long](evtNum)(-1)
    sorted.foreach(e => {
      if (e.eid == 0) {
        timestamps(e.eid) = e.ts
      } else if (timestamps(e.eid -1) > -1 && timestamps(e.eid -1) + window >= e.ts) {
        timestamps(e.eid) = e.ts
      }

      if (timestamps.last > -1) {
        maxStepId = timestamps.length - 1
        return (maxStepId, startTimeStamp)
      }
    })

    maxStepId = timestamps.lastIndexWhere(ts => ts > -1)
    (maxStepId, startTimeStamp)

  }

  override def serialize(buffer: Seq[Event]): Array[Byte] = {
    SerializeUtil.serialize(buffer)
  }

  override def deserialize(storageFormat: Array[Byte]): Seq[Event] = {
    SerializeUtil.deserialize(storageFormat)
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
    for (i <- 0 until attachPropertiesNum) {
      val children = stepIdPropsArray.children.apply(i)
      val nameExprs = children.asInstanceOf[CreateNamedStruct].nameExprs
      val propertyColumn = nameExprs.apply(1)
      val propertyColumnName = propertyColumn.asInstanceOf[Literal].value
      val propertyColumnType = children.dataType.asInstanceOf[StructType].apply(i).dataType
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

case class Event(eid: Int, ts: Long, dim: String, attachPropsArray: Array[Any])

object SerializeUtil {
  import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

  def writeEvent(oss: ObjectOutputStream, event: Event): Unit = {
    oss.writeInt(event.eid)
    oss.writeLong(event.ts)
    if (event.dim == null) {
      oss.writeBoolean(true)
    } else {
      oss.writeBoolean(false)
      oss.writeUTF(event.dim)
    }
    oss.writeObject(event.attachPropsArray)
  }

  def readEvent(ois: ObjectInputStream): Event = {
    val id = ois.readInt()
    val ts = ois.readLong()
    if (ois.readBoolean()) {
      Event(id, ts, null, ois.readObject().asInstanceOf[Array[Any]])
    } else {
      Event(id, ts, ois.readUTF(), ois.readObject().asInstanceOf[Array[Any]])
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
