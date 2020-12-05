import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    public static void main(String[] args) {
        new ConsumerDemoThread().run();
    }

    public ConsumerDemoThread() {

    }

    private void run() {

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        final ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(countDownLatch);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Cauth shutdown hook");
                    myConsumerRunnable.shutDown();
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("application has exited");
                }
        ));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.info("Application is interrupted" ,e);
        }finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private String bootstrapServers = "127.0.0.1:9092";
        private String groupId = "my-sixth-application";
        private String topic = "first_topic";

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

            final Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topics
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {

            try {
                //poll for new data
                while (true) {
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " + record.key() + ", Value:" + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown() {
            // interrup consumer.poll()
            consumer.wakeup();
        }
    }
}

