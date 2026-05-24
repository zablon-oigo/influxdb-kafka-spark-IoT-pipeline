package sensor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

public class SensorProcessor {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("IoTSensorPipeline")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // JSON schema
        StructType schema = new StructType()
                .add("deviceId", DataTypes.StringType)
                .add("temperature", DataTypes.DoubleType)
                .add("humidity", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType);

        // Kafka source
        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "sensor-data")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> parsed = raw
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).alias("data"))
                .select("data.*");

        Dataset<Row> enriched = parsed.withColumn("event_time", current_timestamp());

        Dataset<Row> agg = enriched.groupBy(
                window(col("event_time"), "30 seconds"),
                col("deviceId")
        ).agg(
                avg("temperature").alias("avg_temp"),
                avg("humidity").alias("avg_humidity"),
                count("*").alias("count")
        );

        // InfluxDB
        String token = System.getenv("INFLUX_TOKEN");
        InfluxDBClient influx = InfluxDBClientFactory.create(
                "http://influxdb:8086",
                token.toCharArray(),
                "my-org",
                "sensor-bucket"
        );

        WriteApiBlocking writeApi = influx.getWriteApiBlocking();

        StreamingQuery query = agg.writeStream()
                .outputMode("update")
                .foreachBatch((df, batchId) -> {

                    df.collectAsList().forEach(row -> {

                        String device = row.getAs("deviceId");

                        Double avgTemp = row.getAs("avg_temp");
                        Double avgHum = row.getAs("avg_humidity");
                        Long count = row.getAs("count");

                        Point point = Point.measurement("sensor_metrics")
                                .addTag("device", device)
                                .addField("avg_temp", avgTemp)
                                .addField("avg_humidity", avgHum)
                                .addField("count", count)
                                .time(System.currentTimeMillis(), WritePrecision.MS);

                        writeApi.writePoint(point);
                    });

                    System.out.println("Batch " + batchId + " written to InfluxDB");
                })
                .start();

        System.out.println("Spark streaming running...");
        query.awaitTermination();

        influx.close();
        spark.stop();
    }
}
