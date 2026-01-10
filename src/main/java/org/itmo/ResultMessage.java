package org.itmo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultMessage implements Serializable {
    private String taskId;
    private String taskType;
    private int sectionIndex;
    
    private Long wordCount;
    private Map<String, Integer> topWords;
    private Double sentimentScore;
    private String processedText;
    private List<String> sortedSentences;
    
    public ResultMessage() {
        this.topWords = new HashMap<>();
    }
    
    public ResultMessage(String taskId, String taskType, int sectionIndex) {
        this();
        this.taskId = taskId;
        this.taskType = taskType;
        this.sectionIndex = sectionIndex;
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getTaskType() {
        return taskType;
    }
    
    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }
    
    public int getSectionIndex() {
        return sectionIndex;
    }
    
    public void setSectionIndex(int sectionIndex) {
        this.sectionIndex = sectionIndex;
    }
    
    public Long getWordCount() {
        return wordCount;
    }
    
    public void setWordCount(Long wordCount) {
        this.wordCount = wordCount;
    }
    
    public Map<String, Integer> getTopWords() {
        return topWords;
    }
    
    public void setTopWords(Map<String, Integer> topWords) {
        this.topWords = topWords;
    }
    
    public Double getSentimentScore() {
        return sentimentScore;
    }
    
    public void setSentimentScore(Double sentimentScore) {
        this.sentimentScore = sentimentScore;
    }
    
    public String getProcessedText() {
        return processedText;
    }
    
    public void setProcessedText(String processedText) {
        this.processedText = processedText;
    }
    
    public List<String> getSortedSentences() {
        return sortedSentences;
    }
    
    public void setSortedSentences(List<String> sortedSentences) {
        this.sortedSentences = sortedSentences;
    }
}

