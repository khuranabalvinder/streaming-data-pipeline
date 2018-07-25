package com.free2wheelers.apps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StringType


object StationTranformerApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("StationConsumer2")
      .getOrCreate()

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val result = df.
      select(
        col("key").cast(StringType),   // deserialize keys
        col("value").cast(StringType), // deserialize values
        col("topic"))

    result.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()

  }
}
