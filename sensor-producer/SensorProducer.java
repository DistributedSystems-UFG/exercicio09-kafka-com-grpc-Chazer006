import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

public class SensorProducer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer =
                new KafkaProducer<>(props);

        Random random = new Random();

        while (true) {

            double temperatura = 20 + (40 - 20) * random.nextDouble();

            String evento = "{\"sensorId\":\"sensor-01\",\"temperatura\":"
                    + temperatura + "}";

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("temperatura.raw", evento);

            producer.send(record);

            System.out.println("Evento enviado: " + evento);

            Thread.sleep(3000);
        }
    }
}
