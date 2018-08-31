package com.free2wheelers.apps

import java.util

import org.apache.spark.sql.{ForeachWriter, Row, SaveMode, SparkSession}

class ForeachCsvWriter(path: String) extends ForeachWriter[Row] {
  var csvPath: String = ""

  override def open(partitionId: Long, batchId: Long): Boolean = {
    csvPath = s"$path/$batchId.csv"
    print(csvPath)
    true
  }

  override def process(value: Row): Unit = {
    val sparkSession = SparkSession.builder
      .appName("StationConsumer")
      .getOrCreate()
    sparkSession.createDataFrame(util.Arrays.asList(value), value.schema)
      .repartition(1)
      .write
      .mode(SaveMode.Append)
      .csv(path = csvPath)
  }

  override def close(errorOrNull: Throwable): Unit = {}

}
