package org.itmo;

import java.io.Serializable;
import java.util.UUID;

public class TaskMessage implements Serializable {
    private String taskId;
    private String taskType;
    private String textSection;
    private int sectionIndex;
    private int topN;
    
    public TaskMessage() {
        this.taskId = UUID.randomUUID().toString();
    }
    
    public TaskMessage(String taskType, String textSection, int sectionIndex) {
        this();
        this.taskType = taskType;
        this.textSection = textSection;
        this.sectionIndex = sectionIndex;
    }
    
    public TaskMessage(String taskType, String textSection, int sectionIndex, int topN) {
        this(taskType, textSection, sectionIndex);
        this.topN = topN;
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
    
    public String getTextSection() {
        return textSection;
    }
    
    public void setTextSection(String textSection) {
        this.textSection = textSection;
    }
    
    public int getSectionIndex() {
        return sectionIndex;
    }
    
    public void setSectionIndex(int sectionIndex) {
        this.sectionIndex = sectionIndex;
    }
    
    public int getTopN() {
        return topN;
    }
    
    public void setTopN(int topN) {
        this.topN = topN;
    }
}

