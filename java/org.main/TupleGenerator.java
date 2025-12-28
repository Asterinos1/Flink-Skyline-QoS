package org.main;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;

public class TupleGenerator {
    public static void main(String[] args) {
        //temporary
        //dummy data generator, sends data to topic "input-tuples".
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random r = new Random();

        for (int i = 0; i < 10000000; i++) {
            // Generate 2D or ND data
            String val = i + "," + r.nextDouble() * 1000 + "," + r.nextDouble() * 1000;
            producer.send(new ProducerRecord<>("input-tuples", null, val));
        }
        producer.close();
    }
}