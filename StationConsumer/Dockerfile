FROM p7hb/docker-spark

ADD target/scala-2.11/free2wheelers-station-consumer_2.11-0.0.1.jar /app.jar
COPY spark_sink_csv_complete_provider_2.11-0.1.jar /spark_sink_csv_complete_provider_2.11-0.1.jar
ENTRYPOINT spark-submit --jars /spark_sink_csv_complete_provider_2.11-0.1.jar --class com.free2wheelers.apps.StationApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0  /app.jar



