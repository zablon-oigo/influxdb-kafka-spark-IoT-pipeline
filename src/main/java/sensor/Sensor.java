package sensor;

public class Sensor {

    public String deviceId;
    public Double temperature;
    public Double humidity;
    public Long timestamp;

    public Sensor() {}

    public Sensor(
        String deviceId,
        Double temperature,
        Double humidity,
        Long timestamp
    ) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.humidity = humidity;
        this.timestamp = timestamp;
    }
}