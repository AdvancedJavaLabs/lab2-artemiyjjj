package org.itmo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String RESULT_TOPIC = "result-topic";
    private static final int NUM_WORKERS = 3;
    private static final String INPUT_FILE = "./text/warandpeace.txt";
    private static final String OUTPUT_DIR = "./results";
    
    public static void main(String[] args) {
        String inputFile = args.length >= 1 ? args[0] : INPUT_FILE;
        int numWorkers = NUM_WORKERS;
        // dummy to load class
        TextProcessor.countWords(args[0]);
        
        if (args.length >= 2) {
            try {
                numWorkers = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid value for number of workers, using default: " + NUM_WORKERS);
            }
        }
        try {
            runFullPipeline(inputFile, numWorkers);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void runFullPipeline(String inputFile, int numWorkers) throws Exception {
        
        KafkaSharedResources sharedResources = new KafkaSharedResources(KAFKA_BOOTSTRAP_SERVERS);
        KafkaProducer<String, byte[]> sharedProducer = sharedResources.getSharedProducer();
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregator-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        KafkaConsumer<String, byte[]> sharedAggregatorConsumer = new KafkaConsumer<>(consumerProps);
        sharedAggregatorConsumer.subscribe(Collections.singletonList(RESULT_TOPIC));
        
        FileProducer tempProducer = new FileProducer(sharedProducer, inputFile, 10);
        int sectionsCount = tempProducer.countSections();
        System.out.println("[Setup] File contains " + sectionsCount + " sections\n");
        
        KafkaAggregator aggregator = new KafkaAggregator(
            RESULT_TOPIC, 
            sectionsCount
        );
        
        final KafkaAggregator.AggregatedResults[] aggregatorResults = new KafkaAggregator.AggregatedResults[1];
        final Exception aggregatorException[] = new Exception[1];
        
        Thread aggregatorThread = new Thread(() -> {
            try {
                System.out.println("[Aggregator] Started. Waiting for results");
                aggregator.start(sharedAggregatorConsumer);
                aggregatorResults[0] = aggregator.getResults();
            } catch (Exception e) {
                System.err.println("[Aggregator] Error: " + e.getMessage());
                e.printStackTrace();
                aggregatorException[0] = e;
            }
        });
        aggregatorThread.start();
        
        Thread.sleep(500);
        
        int totalWorkers = numWorkers * FileProducer.TASK_TYPES.length;
        CountDownLatch workersReadyLatch = new CountDownLatch(totalWorkers);
        List<KafkaConsumerWorker> workers = new ArrayList<>();
        List<Thread> workerThreads = new ArrayList<>();
        
        for (String taskTopic : FileProducer.TASK_TYPES) {
            for (int i = 0; i < numWorkers; i++) {
                final String topic = taskTopic;
                final int workerNum = i + 1;
                final String workerId = "Worker-" + topic + "-" + workerNum;
                final String consumerGroupId = "worker-group-" + topic;
                
                KafkaConsumerWorker worker = new KafkaConsumerWorker(
                    KAFKA_BOOTSTRAP_SERVERS, 
                    topic, 
                    RESULT_TOPIC, 
                    workerId, 
                    consumerGroupId,
                    sharedProducer,
                    workersReadyLatch
                );
                workers.add(worker);
                
                Thread workerThread = new Thread(() -> {
                    try {
                        worker.start();
                    } catch (Exception e) {
                        System.err.println("[" + workerId + "] Error: " + e.getMessage());
                        e.printStackTrace();
                        workersReadyLatch.countDown();
                    }
                });
                workerThreads.add(workerThread);
                workerThread.start();
            }
        }
        
        System.out.println("Waiting for all " + totalWorkers + " workers to subscribe to topics\n");
        boolean allReady = workersReadyLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        if (!allReady) {
            System.err.println("Warning: Not all workers subscribed within timeout");
        } else {
            System.out.println("All workers successfully subscribed\n");
        }
        
        Thread producerThread = new Thread(() -> {
            try {
                System.out.println("[Producer] Reading file and splitting into sections");
                FileProducer producer = new FileProducer(sharedProducer, inputFile, 10);
                int actualSectionsCount = producer.prepareTextAndProduce();
                System.out.println("[Producer] Finished. Sent " + actualSectionsCount + " sections.");
            } catch (Exception e) {
                System.err.println("[Producer] Error: " + e.getMessage());
                e.printStackTrace();
            }
        });
        producerThread.start();
        producerThread.join();
        aggregatorThread.join();
        
        if (aggregatorException[0] != null) {
            throw aggregatorException[0];
        }
        
        if (aggregatorResults[0] == null) {
            throw new RuntimeException("Aggregator failed to get results");
        }
        
        Thread summarizerThread = new Thread(() -> {
            try {
                Summarizer.saveResults(aggregatorResults[0], OUTPUT_DIR);
            } catch (Exception e) {
                System.err.println("[Summarizer] Error: " + e.getMessage());
                e.printStackTrace();
            }
        });
        summarizerThread.start();
        summarizerThread.join();

        for (KafkaConsumerWorker worker : workers) {
            worker.shutdown();
        }
        aggregator.shutdown();
        Thread.sleep(2000);
        for (Thread workerThread : workerThreads) {
            try {
                workerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        sharedAggregatorConsumer.close();
        sharedResources.close();
    }
}
