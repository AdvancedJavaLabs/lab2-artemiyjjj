package org.itmo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class KafkaAggregator {
    private final String resultTopic;
    private final int expectedSections;
    
    private final Map<String, Long> wordCounts = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Integer>> topWordsMap = new ConcurrentHashMap<>(); 
    private final List<Double> sentimentScores = Collections.synchronizedList(new ArrayList<>());
    private final Map<Integer, String> processedTexts = new ConcurrentHashMap<>(); 
    private final Map<Integer, List<String>> sortedSentencesMap = new ConcurrentHashMap<>(); 
    
    private final Set<String> completedTasks = ConcurrentHashMap.newKeySet();
    private final CountDownLatch completionLatch;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private KafkaConsumer<String, byte[]> sharedConsumer;
    
    public KafkaAggregator(String resultTopic, int expectedSections) {
        this.resultTopic = resultTopic;
        this.expectedSections = expectedSections;
        
        // Initialize latch for waiting all results
        // Each task type should be executed for each section
        this.completionLatch = new CountDownLatch(expectedSections * FileProducer.TASK_TYPES.length);
    }
    
    public void start(KafkaConsumer<String, byte[]> sharedConsumer) throws InterruptedException {
        this.sharedConsumer = sharedConsumer;

        while (running.get()) {
            ConsumerRecords<String, byte[]> records = sharedConsumer.poll(Duration.ofMillis(100));
            
            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    byte[] messageBody = record.value();
                    ResultMessage result = MessageSerializer.deserializeResult(messageBody);
                    String taskKey = result.getTaskType() + "_" + result.getSectionIndex();
                    synchronized (this) {
                        if (completedTasks.contains(taskKey)) {
                            System.out.println("[Aggregator] Duplicate result ignored: " + taskKey);
                            sharedConsumer.commitSync();
                            continue;
                        }
                        completedTasks.add(taskKey);
                        aggregateResult(result);
                        System.out.println("[Aggregator] Aggregated result: " + result.getTaskType() + 
                                            " (section " + result.getSectionIndex() + 
                                            ", total completed: " + completedTasks.size() + ")");
                        completionLatch.countDown();
                        if (completionLatch.getCount() == 0) {
                            sharedConsumer.commitSync();
                            return;
                        }
                    }
                    sharedConsumer.commitSync();
                } catch (Exception e) {
                    System.err.println("[Aggregator] Error processing result: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            if (completionLatch.getCount() == 0) {
                break;
            }
        } 
    }
    
    public void shutdown() {
        running.set(false);
        if (sharedConsumer != null) {
            sharedConsumer.wakeup();
        }
    }
    
    private synchronized void aggregateResult(ResultMessage result) {
        String taskType = result.getTaskType();
        switch (taskType) {
            case "WORD_COUNT":
                if (result.getWordCount() != null) {
                    wordCounts.merge(taskType, result.getWordCount(), Long::sum);
                }
                break;
                
            case "TOP_N":
                if (result.getTopWords() != null) {
                    Map<String, Integer> aggregated = topWordsMap.computeIfAbsent(
                        taskType, k -> new HashMap<>()
                    );
                    result.getTopWords().forEach((word, count) ->
                        aggregated.merge(word, count, Integer::sum)
                    );
                }
                break;
                
            case "SENTIMENT":
                if (result.getSentimentScore() != null) {
                    sentimentScores.add(result.getSentimentScore());
                }
                break;
                
            case "NAME_REPLACE":
                if (result.getProcessedText() != null) {
                    processedTexts.put(result.getSectionIndex(), result.getProcessedText());
                }
                break;
                
            case "SORT_SENTENCES":
                if (result.getSortedSentences() != null) {
                    sortedSentencesMap.put(result.getSectionIndex(), result.getSortedSentences());
                }
                break;
        }
    }
    
    public AggregatedResults getResults() {
        return new AggregatedResults(
            wordCounts.getOrDefault("WORD_COUNT", 0L),
            getTopNWordsFromMap(topWordsMap.getOrDefault("TOP_N", new HashMap<>()), 10),
            calculateAverageSentiment(),
            combineProcessedTexts(),
            combineSortedSentences()
        );
    }
    
    private Map<String, Integer> getTopNWordsFromMap(Map<String, Integer> wordMap, int n) {
        return wordMap.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .limit(n)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1,
                LinkedHashMap::new
            ));
    }
    
    private double calculateAverageSentiment() {
        if (sentimentScores.isEmpty()) {
            return 0.0;
        }
        return sentimentScores.stream()
            .mapToDouble(Double::doubleValue)
            .average()
            .orElse(0.0);
    }
    
    private String combineProcessedTexts() {
        return processedTexts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .collect(Collectors.joining("\n\n"));
    }
    
    private List<String> combineSortedSentences() {
        return sortedSentencesMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .flatMap(entry -> entry.getValue().stream())
            .sorted(Comparator.comparingInt(String::length))
            .collect(Collectors.toList());
    }
    
    public static class AggregatedResults {
        private final long totalWordCount;
        private final Map<String, Integer> topWords;
        private final double averageSentiment;
        private final String processedText;
        private final List<String> sortedSentences;
        
        public AggregatedResults(long totalWordCount, Map<String, Integer> topWords,
                                double averageSentiment, String processedText,
                                List<String> sortedSentences) {
            this.totalWordCount = totalWordCount;
            this.topWords = topWords;
            this.averageSentiment = averageSentiment;
            this.processedText = processedText;
            this.sortedSentences = sortedSentences;
        }
        
        public long getTotalWordCount() { return totalWordCount; }
        public Map<String, Integer> getTopWords() { return topWords; }
        public double getAverageSentiment() { return averageSentiment; }
        public String getProcessedText() { return processedText; }
        public List<String> getSortedSentences() { return sortedSentences; }
    }
}

