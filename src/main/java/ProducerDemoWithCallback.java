import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //Producer Properties
        String bootstrapServers = "127.0.0.1:9092";
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        //key-value
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World");

        //send data - asnyc
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is successfully sent or an exception is thrown
                if (e == null) {
                    //the record was successfully sent
                    logger.info("Recieved new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });

        //flush data
        producer.flush();
        //flush data and close
        producer.close();
    }
}
