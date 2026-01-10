package org.itmo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerWorker {
    private final String bootstrapServers;
    private final String taskTopic;
    private final String resultTopic;
    private final String workerId;
    private final String consumerGroupId;
    private final CountDownLatch readyLatch;
    private final KafkaProducer<String, byte[]> sharedProducer;
    private KafkaConsumer<String, byte[]> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private static final int TOP_N_DEFAULT = 10;
    
    public KafkaConsumerWorker(String bootstrapServers, String taskTopic, String resultTopic, 
                              String workerId, String consumerGroupId, 
                              KafkaProducer<String, byte[]> sharedProducer,
                              CountDownLatch readyLatch) {
        this.bootstrapServers = bootstrapServers;
        this.taskTopic = taskTopic;
        this.resultTopic = resultTopic;
        this.workerId = workerId;
        this.consumerGroupId = consumerGroupId;
        this.sharedProducer = sharedProducer;
        this.readyLatch = readyLatch;
    }
    
    public void start() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000);
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000);
        
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(taskTopic));
        
        System.out.println("[" + workerId + "] Worker started. Subscribed to topic: " + taskTopic);
        
        if (readyLatch != null) {
            readyLatch.countDown();
        }
        
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(10000));
                
                for (ConsumerRecord<String, byte[]> record : records) {
                    long taskStartTime = System.currentTimeMillis();
                    try {
                        System.out.println("[" + workerId + "] Received record from topic: " + record.topic() + 
                                         " partition: " + record.partition() + 
                                         " offset: " + record.offset() + 
                                         " (size: " + record.value().length + " bytes)");
                        
                        byte[] messageBody = record.value();
                        TaskMessage task = MessageSerializer.deserializeTask(messageBody);
                        String text = task.getTextSection();
                        ResultMessage result = processTask(task);
                        long processingTime = System.currentTimeMillis() - taskStartTime;
                        byte[] resultBytes = MessageSerializer.serialize(result);
                        System.out.println("[" + workerId + "] Publishing result to topic: " + resultTopic);
                        ProducerRecord<String, byte[]> resultRecord = new ProducerRecord<>(
                            resultTopic, 
                            result.getTaskId(), 
                            resultBytes
                        );
                        sharedProducer.send(resultRecord);
                        sharedProducer.flush();
                        consumer.commitSync();
                        System.out.println("[" + workerId + "] Task completed: " + task.getTaskType() + 
                                         " (section " + task.getSectionIndex() + ") in " + processingTime + "ms");
                    } catch (IllegalArgumentException e) {
                        System.err.println("[" + workerId + "] Error message: " + e.getMessage());
                        e.printStackTrace();
                        System.err.flush();
                    }
                }
            }
            System.out.println("[" + workerId + "] Has been interrupted ");
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            System.out.println("[" + workerId + "] Consumer wakeup requested");
        } catch (Exception e) {
            System.err.println("[" + workerId + "] Error message: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (consumer != null) {
                    consumer.close();
                }
            } catch (Exception e) {
                System.err.println("[" + workerId + "] Error closing consumer: " + e.getMessage());
            }
        }
    }
    
    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
    }
    
    private ResultMessage processTask(TaskMessage task) throws IllegalArgumentException {
        ResultMessage result = new ResultMessage(
            task.getTaskId(),
            task.getTaskType(),
            task.getSectionIndex()
        );
        
        String text = task.getTextSection();
        if (text == null) {
            text = "";
        }
        int textLength = text.length();
        
        try {
            switch (task.getTaskType()) {
                case "WORD_COUNT":
                    long wordCount = TextProcessor.countWords(text);
                    result.setWordCount(wordCount);
                    System.out.println("[" + workerId + "] WORD_COUNT result: " + wordCount + " words");
                    break;
                    
                case "TOP_N":
                    int topN = task.getTopN() > 0 ? task.getTopN() : TOP_N_DEFAULT;
                    Map<String, Integer> topWords = TextProcessor.findTopNWords(text, topN);
                    result.setTopWords(topWords);
                    System.out.println("[" + workerId + "] TOP_N result: " + topWords.size() + " unique words");
                    break;
                    
                case "SENTIMENT":
                    double sentiment = TextProcessor.analyzeSentiment(text);
                    result.setSentimentScore(sentiment);
                    System.out.println("[" + workerId + "] SENTIMENT result: " + String.format("%.4f", sentiment));
                    break;
                    
                case "NAME_REPLACE":
                    String processedText = TextProcessor.replaceNames(text, "Parallels");
                    result.setProcessedText(processedText);
                    System.out.println("[" + workerId + "] NAME_REPLACE result: processed text length " + processedText.length() + " chars");
                    break;
                    
                case "SORT_SENTENCES":
                    List<String> sortedSentences = TextProcessor.sortSentencesByLength(text);
                    result.setSortedSentences(sortedSentences);
                    System.out.println("[" + workerId + "] SORT_SENTENCES result: " + sortedSentences.size() + " sentences");
                    break;
                    
                default:
                    throw new IllegalArgumentException("Unknown task type: " + task.getTaskType());
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            throw e;
        }
        
        return result;
    }
}

