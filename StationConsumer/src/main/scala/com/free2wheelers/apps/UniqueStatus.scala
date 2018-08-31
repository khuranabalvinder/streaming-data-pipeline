package com.free2wheelers.apps

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class UniqueStatus extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StationStatusSchema.transformedSchema

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("max_time", LongType)
      :: StructField("row", StationStatusSchema.transformedSchema)
      :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: StructType = StationStatusSchema.transformedSchema

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputTime = input.getAs[Long](5)
    if (inputTime > buffer.getAs[Long](0)) {
      buffer(0) = inputTime
      buffer(1) = input
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val inputTime = buffer2.getAs[Long](0)
    if (inputTime > buffer1.getAs[Long](0)) {
      buffer1(0) = inputTime
      buffer1(1) = buffer2.get(1)
    }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row) = {
    buffer.getAs[Row](1)
  }
}
