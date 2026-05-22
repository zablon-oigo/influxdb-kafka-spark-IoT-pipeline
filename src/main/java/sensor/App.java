package sensor;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class App {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String TOPIC = "sensor-data";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        System.out.println("Sensor producer started");

        while (true) {

            Sensor sensor = new Sensor(
                    "sensor-" + random.nextInt(2),
                    20 + random.nextDouble() * 15,
                    30 + random.nextDouble() * 50,
                    System.currentTimeMillis()
            );

            String json = mapper.writeValueAsString(sensor);

            producer.send(new ProducerRecord<>(
                    TOPIC,
                    sensor.deviceId,
                    json
            ));

            System.out.printf(
                    "Sent: %s Temp=%.2f Humidity=%.2f%n",
                    sensor.deviceId,
                    sensor.temperature,
                    sensor.humidity
            );

            Thread.sleep(1000);
        }
    }
}