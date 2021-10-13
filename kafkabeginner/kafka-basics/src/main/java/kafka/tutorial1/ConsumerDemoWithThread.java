package kafka.tutorial1;

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

/**
 * @author viveksoni100
 */
public class ConsumerDemoWithThread {

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {

    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        final String bootstrapServer = "localhost:9092";
        final String groupId = "my-sixth-application";
        final String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread!");
        Runnable myConsumerThread = new ConsumerThread(logger, bootstrapServer, groupId, topic, latch);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application is closed!");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted : ", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        // create consumer
        KafkaConsumer<String, String> consumer;
        Logger logger;

        public ConsumerThread(Logger logger, String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            this.logger = logger;

            // create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + "\n" +
                                "Value : " + record.value() + "\n" +
                                "Offset : " + record.offset() + "\n" +
                                "Partition : " + record.partition());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code, we're done
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw WakeUpException
            consumer.wakeup();
        }
    }
}
