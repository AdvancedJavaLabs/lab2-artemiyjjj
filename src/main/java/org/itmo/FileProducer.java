package org.itmo;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileProducer extends KafkaProducerBase {
    private final String filename;
    private final int topN;
    public static final String[] TASK_TYPES = {
        "WORD_COUNT",
        "TOP_N",
        "SENTIMENT",
        "NAME_REPLACE",
        "SORT_SENTENCES"
    };
    // public static final String[] TASK_TOPICS = {
    //     "task-topic-word-count",
    //     "task-topic-top-n",
    //     "task-topic-sentiment",
    //     "task-topic-name-replace",
    //     "task-topic-sort-sentences"
    // };

    public FileProducer(KafkaProducer<String, byte[]> sharedProducer, String filename) {
        super(sharedProducer);
        this.filename = filename;
        this.topN = 10;
    }

    public FileProducer(KafkaProducer<String, byte[]> sharedProducer, String filename, int topN) {
        super(sharedProducer);
        this.filename = filename;
        this.topN = topN;
    }

    /**
     * Разбивает файл на секции (по параграфам) и отправляет задачи в Kafka topics
     * @return количество созданных секций
     */
    public int prepareTextAndProduce() throws IOException {
        Path file;

        try {
            file = Paths.get(this.filename);
            if (!Files.isRegularFile(file)) {
                throw new FileNotFoundException();
            }
        } catch (InvalidPathException e) {
            throw new IOException("Failed to open file " + filename + "\n" + e.getMessage());
        } catch (FileNotFoundException e) {
            throw new IOException("File " + filename + " is not present or a directory");
        }

        // Читаем весь файл
        String fileContent = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        
        // Разбиваем на секции по параграфам (двойной перенос строки) или предложениям
        List<String> sections = splitIntoSections(fileContent);
        
        System.out.println("[Producer] Split file into " + sections.size() + " sections");
        
        // Создаем и отправляем задачи для каждой секции
        try {
            int taskCounter = 0;
            for (int sectionIndex = 0; sectionIndex < sections.size(); sectionIndex++) {
                String section = sections.get(sectionIndex);
                // System.out.println("[Producer] Processing section " + sectionIndex + " (length: " + section.length() + " chars)");
                
                // Отправляем задачи всех типов для каждой секции
                for (int taskTypeIndex = 0; taskTypeIndex < TASK_TYPES.length; taskTypeIndex++) {
                    String taskType = TASK_TYPES[taskTypeIndex];
                    String taskTopic = TASK_TYPES[taskTypeIndex];
                    
                    TaskMessage task;
                    if ("TOP_N".equals(taskType)) {
                        task = new TaskMessage(taskType, section, sectionIndex, topN);
                    } else {
                        task = new TaskMessage(taskType, section, sectionIndex);
                    }
                    
                    byte[] messageBytes = MessageSerializer.serialize(task);
                    // Use taskId as key for partition distribution
                    String key = task.getTaskId();
					taskCounter++;
                    // System.out.println("[Producer] Sending task #" + (taskCounter) + ": " + taskType + 
                    //                  " for section " + sectionIndex + " (message size: " + messageBytes.length + " bytes)");
                    super.produce(taskTopic, key, messageBytes);
                }
            }
            
            // Ждем завершения всех отправок
            super.producer.flush();
            System.out.println("[Producer] All tasks sent to Kafka topics (" + (sections.size() * TASK_TYPES.length) + " tasks total)");
            
        } catch (Exception e) {
            throw new IOException("Error while sending messages to Kafka: " + e.getMessage(), e);
        }
        
        return sections.size();
    }
    
    /**
     * Подсчитывает количество секций в файле без разбиения
     */
    public int countSections() throws IOException {
        Path file;

        try {
            file = Paths.get(this.filename);
            if (!Files.isRegularFile(file)) {
                throw new FileNotFoundException();
            }
        } catch (InvalidPathException e) {
            throw new IOException("Failed to open file " + filename + "\n" + e.getMessage());
        } catch (FileNotFoundException e) {
            throw new IOException("File " + filename + " is not present or a directory");
        }

        // Читаем весь файл
        String fileContent = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        
        // Разбиваем на секции по параграфам (двойной перенос строки) или предложениям
        List<String> sections = splitIntoSections(fileContent);
        
        return sections.size();
    }
    
    /**
     * Разбивает текст на секции по параграфам
     */
    private List<String> splitIntoSections(String text) {
        List<String> sections = new ArrayList<>();
        
        // Разбиваем по двойному переносу строки (параграфы)
        String[] paragraphs = text.split("\\n\\s*\\n+");
        
        // Если получилось слишком мало параграфов, разбиваем по предложениям
        if (paragraphs.length < 2) {
            paragraphs = text.split("[.!?]+\\s+");
        }
        
        // Объединяем очень короткие секции
        StringBuilder currentSection = new StringBuilder();
        int minSectionLength = 100; // минимальная длина секции
        
        for (String paragraph : paragraphs) {
            String trimmed = paragraph.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            
            if (currentSection.length() > 0 && 
                currentSection.length() + trimmed.length() < minSectionLength) {
                // Добавляем к текущей секции
                currentSection.append(" ").append(trimmed);
            } else {
                // Сохраняем предыдущую секцию и начинаем новую
                if (currentSection.length() > 0) {
                    sections.add(currentSection.toString());
                }
                currentSection = new StringBuilder(trimmed);
            }
        }
        
        // Добавляем последнюю секцию
        if (currentSection.length() > 0) {
            sections.add(currentSection.toString());
        }
        
        // Если всё ещё пусто, используем весь текст как одну секцию
        if (sections.isEmpty()) {
            sections.add(text.trim());
        }
        
        return sections;
    }
}
