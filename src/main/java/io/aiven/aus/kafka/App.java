package io.aiven.aus.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import io.github.cdimascio.dotenv.Dotenv;

public class App {

    public static void main(String[] args) {
        App a = new App();
        a.run();
    }

    public void run() {
        Dotenv dotenv = Dotenv.load();
      
        String topic = dotenv.get("TOPIC_NAME");
        String sasl_username = dotenv.get("SASL_USER");
        String sasl_password = dotenv.get("SASL_PASS");
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username,sasl_password);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",dotenv.get("BOOTSTRAP_SERVERS"));
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("sasl.jaas.config", jaasConfig);
        properties.setProperty("ssl.endpoint.identification.algorithm", "");
        properties.setProperty("ssl.truststore.type", "jks");
        properties.setProperty("ssl.truststore.location", dotenv.get("TRUST_STORE_LOCATION"));
        properties.setProperty("ssl.truststore.password", dotenv.get("TRUST_STORE_PASS"));
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties, null, null);

        try {
            int i = 0;
            System.out.println("Starting to send messages");
            while (true) {
                String message = "Simple test message number " + i++;
                System.out.println("trying to send");
                ProducerRecord<String,String> pr = new ProducerRecord<String, String>(topic, null, null, jaasConfig, message, null);
                producer.send(pr, new Callback() {
                    public void onCompletion(RecordMetadata rm, Exception e) {
                        System.out.println("have sent");
                        if (e != null) {
                            System.err.println(e.getMessage());
                            e.printStackTrace();
                        } else {
                            System.out.println(String.format("we have offest %d in topic %s",rm.offset(), rm.topic()));
                        }
                    }
                });
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    return;
                }
            }
        } finally {
            producer.close();
        }

    }
}