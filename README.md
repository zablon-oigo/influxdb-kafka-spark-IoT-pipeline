## Building a Real-Time IoT Sensor Pipeline using Kafka, Spark Structured Streaming, and InfluxDB with time-window aggregations.


![workflow](https://github.com/zablon-oigo/influxdb-kafka-spark-IoT-pipeline/actions/workflows/ci.yaml/badge.svg)
![Java](https://img.shields.io/badge/Java-17+-ED8B00?logo=openjdk&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-3.8+-C71A36?logo=apachemaven&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5+-E25A1C?logo=apachespark&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-2.x-22ADF6?logo=influxdb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Latest-2496ED?logo=docker&logoColor=white)
![IoT Sensors](https://img.shields.io/badge/IoT-Sensor%20Streaming-4CAF50?logo=iota&logoColor=white)
![Structured Streaming](https://img.shields.io/badge/Spark-Structured%20Streaming-F88909?logo=apache-spark&logoColor=white)
![Time Series](https://img.shields.io/badge/Analytics-Time--Series-blue)


This project demonstrates how to build a real-time IoT sensor data pipeline using Apache Kafka, Spark Structured Streaming, and InfluxDB.

The pipeline ingests IoT sensor events, processes streaming data with Spark, performs time-window aggregations, and stores processed metrics in InfluxDB for real-time analytics and SQL-style querying.


#### Architecture Diagram
<img width="1017" height="315" alt="iot" src="https://github.com/user-attachments/assets/99246131-debd-4488-91c6-adaf6b9641a3" />


#### Configure InfluxDB

Create Organization and Bucket

```sh
docker exec -it influxdb influx setup \
  --org my-org \
  --bucket sensor-bucket \
  --username admin \
  --password password \
  --retention 1w \
  --force
```

#### Verify Bucket Creation

```sh
docker exec -it influxdb influx bucket list --org my-org
```


#### Create an InfluxDB Authentication Token

Generate a token with read/write access to the bucket:
```sh
docker exec -it influxdb influx auth create \
  --org my-org \
  --description "Spark Streaming Token" \
  --read-bucket <BUCKET_ID>  \
  --write-bucket <BUCKET_ID> 
```
> Replace <BUCKET_ID> with the bucket ID from the previous command.


#### Package the Java application using Maven:

```sh
maven clean package -DskipTests
```
This generates the JAR file:

> target/sensor-1.0-SNAPSHOT.jar

#### Copy the JAR into the Spark Container

```sh
docker cp \
target/sensor-1.0-SNAPSHOT.jar \
spark-master:/opt/spark/
```

#### Access the Spark Container

Inside the Spark container, export your InfluxDB token:

```sh
docker exec -it spark-master bash
```
#### Configure the InfluxDB Token

```sh
export INFLUX_TOKEN="YOUR_INFLUXDB_TOKEN"
```
> Replace YOUR_INFLUXDB_TOKEN with the token generated from InfluxDB authentication.


#### Submit the Spark Streaming Job

Run the Spark Structured Streaming application:

```sh
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class sensor.SensorProcessor \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.influxdb:influxdb-client-java:8.0.0 \
  --conf spark.jars.ivy=/tmp/ivy \
  /opt/spark/sensor-1.0-SNAPSHOT.jar
```
#### Monitor Spark Logs

```sh
docker logs -f spark-master
```

#### Monitor InfluxDB Logs

```sh
docker logs -f influxdb
```

#### View Kafka Topics
```sh
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```


