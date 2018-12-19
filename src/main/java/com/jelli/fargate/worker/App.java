package com.jelli.fargate.worker;

import java.util.Collections;
import java.util.Properties;
import java.util.Vector;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Hello world!
 *
 */
public class App implements Runnable {
    private final static String TOPIC = "worker";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private final String name;
    private final long nanoSeconds;

    public App(String name, long seconds) {
        this.name = name;
        nanoSeconds = seconds * 1000000000l;
    }

    @Override
    public void run() {
        System.out.println(name + " starting. Will run " + nanoSeconds / 1000000000 + " seconds.");
        long start = System.nanoTime();
        long end = System.nanoTime();
        long remaining = nanoSeconds + start - end;
        while (remaining > 0) {
            long data = start;
            // Consume some CPU power
            for (int i = 0; i < 1000000000l; ++i) {
                data = data ^ (data << 2);
            }
            end = System.nanoTime();
            remaining = nanoSeconds + start - end;
            if (remaining > 0) {
                System.out.println(name + " " + remaining + " remaining.");
            }
        }
        System.out.println(name + " finished. ");
    }

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        Vector<Thread> workers = new Vector<>();
        Vector<Thread> finished = new Vector<>();

        while (true) {
            finished.clear();
            @SuppressWarnings("deprecation")
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                Thread.sleep(1000l);
            }

            // Retrieve data from Kafka and start worker threads
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
                        record.partition(), record.offset());
                Long seconds = Long.parseLong(record.value());
                String name = "T" + record.value();
                Thread t = new Thread(new App(name, seconds));
                t.setName(name);
                t.start();
                workers.add(t);
            });

            consumer.commitAsync();

            workers.forEach(t -> {
                if (!t.isAlive()) {
                    System.out.println("Reap " + t.getName());
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    finished.add(t);
                }
            });
            workers.removeAll(finished);
        }
//        consumer.close();
//        System.out.println("DONE");
    }

    public static void main(String[] args) throws InterruptedException {
        runConsumer();
    }
}
