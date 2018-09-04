package com.free2wheelers.apps

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StationStatusTransformation {

  def statusJson2DF(jsonDF: DataFrame): DataFrame = {
    jsonDF
      .select(from_json(col("raw_payload"), StationStatusSchema.schema).as("station_status"))
      .select(col("station_status.payload.data.stations") as "stations", col("station_status.payload.last_updated") as "last_updated")
      .select(explode(col("stations")) as "station", col("last_updated"))
      .select(col("station.station_id") as "station_id"
        , col("station.num_bikes_available") + col("station.num_ebikes_available") as "bikes_available"
        , col("station.num_docks_available") as "docks_available"
        , col("station.is_renting") === 1 as "is_renting"
        , col("station.is_returning") === 1 as "is_returning"
        , col("last_updated"))
  }

  case class Station(
                      station_id: String,
                      name: String,
                      latitude: Double,
                      longitude: Double
                    )

  val toStation: String => Seq[Station] = raw_payload => {
    val json = JSON.parseFull(raw_payload)
    val metadata = json.get.asInstanceOf[Map[String, Any]]("metadata").asInstanceOf[Map[String, String]]
    val producerId = metadata("producer_id")

    val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
    producerId match {
      case "producer_station_information" => extractNycStation(payload)
      case "producer_station_information-san_francisco" => extractSFStation(payload)
    }
  }

  case class Status(
                     bikes_available: Integer,
                     docks_available: Integer,
                     is_renting: Boolean,
                     is_returning: Boolean,
                     last_updated: Long,
                     station_id: String
                   )

  val toStatus: String => Seq[Status] = raw_payload => {
    val json = JSON.parseFull(raw_payload)
    val metadata = json.get.asInstanceOf[Map[String, Any]]("metadata").asInstanceOf[Map[String, String]]
    val producerId = metadata("producer_id")

    val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
    producerId match {
      case "producer_station_information-san_francisco" => extractSFStationStatus(payload)
      case "producer_station_status" => extractNycStationStatus(payload)
    }
  }

  private def extractNycStationStatus(payload: Any) = {
    val data = payload.asInstanceOf[Map[String, Any]]("data")

    val lastUpdated = payload.asInstanceOf[Map[String, Any]]("last_updated").asInstanceOf[Double].toLong

    val stations: Any = data.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .map(x => {
        Status(
          x("num_bikes_available").asInstanceOf[Double].toInt,
          x("num_docks_available").asInstanceOf[Double].toInt,
          x("is_renting").asInstanceOf[Double] == 1,
          x("is_returning").asInstanceOf[Double] == 1,
          lastUpdated,
          x("station_id").asInstanceOf[String]
        )
      })
  }

  private def extractSFStationStatus(payload: Any) = {

    val network: Any = payload.asInstanceOf[Map[String, Any]]("network")

    val stations: Any = network.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .map(x => {
        val str = x("timestamp").asInstanceOf[String]


        val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSSSSS'Z'")
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
        val parsedDate = dateFormat.parse(str)


        Status(
          x("free_bikes").asInstanceOf[Double].toInt,
          x("empty_slots").asInstanceOf[Double].toInt,
          x("extra").asInstanceOf[Map[String, Any]]("renting").asInstanceOf[Double] == 1,
          x("extra").asInstanceOf[Map[String, Any]]("returning").asInstanceOf[Double] == 1,
          parsedDate.getTime / 1000,
          x("id").asInstanceOf[String]
        )
      })
  }

  private def extractNycStation(payload: Any) = {
    val data = payload.asInstanceOf[Map[String, Any]]("data")

    val stations: Any = data.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .map(x => {
        Station(
          x("station_id").asInstanceOf[String],
          x("name").asInstanceOf[String],
          x("lat").asInstanceOf[Double],
          x("lon").asInstanceOf[Double]
        )
      })
  }

  private def extractSFStation(payload: Any): Seq[Station] = {
    val network = payload.asInstanceOf[Map[String, Any]]("network")

    val stations = network.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .map(x => {
        Station(
          x("id").asInstanceOf[String],
          x("name").asInstanceOf[String],
          x("latitude").asInstanceOf[Double],
          x("longitude").asInstanceOf[Double]
        )
      })
  }

  def informationJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStationFn: UserDefinedFunction = udf(toStation)

    import spark.implicits._
    jsonDF.select(explode(toStationFn(jsonDF("raw_payload"))) as "station")
      .select($"station.*")
  }

  def statusInformationJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(toStatus)

    import spark.implicits._

    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }

  case class StationStatus(station_id: String, bikes_available: String,
                           docks_available: Double, is_renting: Boolean, is_returning: Boolean,
                           last_updated: Long)

  def informationJson2DF(jsonDF: DataFrame): DataFrame = {
    jsonDF
      .select(from_json(col("raw_payload"), StationInformationSchema.schema).as("station_information"))
      .select(col("station_information.payload.data.stations") as "stations")
      .select(explode(col("stations")) as "station")
      .select(
        col("station.station_id") as "station_id",
        col("station.name") as "name",
        col("station.lat") as "latitude",
        col("station.lon") as "longitude",
        col("last_updated")
      )

  }

  implicit class StatusTransformationDF(dataFrame: DataFrame) {
    def convertTimestamp(implicit spark: SparkSession) = {
      import spark.implicits._
      dataFrame.withColumn("timestamp", to_timestamp(from_unixtime($"last_updated")))
    }

    def getLatestStatusDF(implicit spark: SparkSession) = {
      import spark.implicits._

      val uniqueStatus = new UniqueStatus

      dataFrame
        .withWatermark("timestamp", "10 seconds")
        .groupBy(
          window($"timestamp", "10 seconds"),
          $"station_id")
        .agg(uniqueStatus(
          $"station_id",
          $"bikes_available",
          $"docks_available",
          $"is_renting",
          $"is_returning",
          $"last_updated") as "status")
        .select($"status.*")
        .as[StationStatus]
    }
  }

  implicit class StatusTransformation(dataset: Dataset[StationStatus]) {
    def getLatestStatus(implicit spark: SparkSession): Dataset[StationStatus] = {
      import spark.implicits._
      val value = dataset
        .groupByKey(_.station_id)
        .reduceGroups((x, y) => if (x.last_updated > y.last_updated) x else y)
      value.map(row => row._2)
    }
  }

}
