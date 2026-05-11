import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.*;

public class ProcessorService {

    public static void main(String[] args) {

        Properties consumerProps = new Properties();

        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "processor-group");
        consumerProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumerProps);

        consumer.subscribe(List.of("temperatura.raw"));

        Properties producerProps = new Properties();

        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(producerProps);

        List<Double> temperaturas = new ArrayList<>();

        while (true) {

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {

                String value = record.value();

                double temperatura =
                        Double.parseDouble(value.split(":")[2]
                        .replace("}", ""));

                temperaturas.add(temperatura);

                double media = temperaturas.stream()
                        .mapToDouble(Double::doubleValue)
                        .average()
                        .orElse(0);

                double max = temperaturas.stream()
                        .mapToDouble(Double::doubleValue)
                        .max()
                        .orElse(0);

                double min = temperaturas.stream()
                        .mapToDouble(Double::doubleValue)
                        .min()
                        .orElse(0);

                String resultado = "{\"sensorId\":\"sensor-01\"," +
                        "\"media\":" + media + "," +
                        "\"maxima\":" + max + "," +
                        "\"minima\":" + min + "}";

                producer.send(
                        new ProducerRecord<>("temperatura.media", resultado)
                );

                System.out.println("Métrica enviada: " + resultado);
            }
        }
    }
}
