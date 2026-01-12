package org.itmo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Summarizer {
    
    public static void saveResults(KafkaAggregator.AggregatedResults results, String outputDir) throws IOException {
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());        
        if (!Files.exists(Paths.get(outputDir))) {
            Files.createDirectories(Paths.get(outputDir));
        }
        saveJsonReport(results, outputDir + "/report_" + timestamp + ".json");
        saveTextReport(results, outputDir + "/report_" + timestamp + ".txt");
        if (results.getProcessedText() != null && !results.getProcessedText().isEmpty()) {
            Files.write(
                Paths.get(outputDir + "/processed_text_" + timestamp + ".txt"),
                results.getProcessedText().getBytes()
            );
        }
        
        if (results.getSortedSentences() != null && !results.getSortedSentences().isEmpty()) {
            String sentencesText = String.join("\n", results.getSortedSentences());
            Files.write(
                Paths.get(outputDir + "/sorted_sentences_" + timestamp + ".txt"),
                sentencesText.getBytes()
            );
        }
        
        System.out.println("Results saved to directory: " + outputDir);
    }
    
    private static void saveJsonReport(KafkaAggregator.AggregatedResults results, String filename) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("\t\"totalWordCount\": ").append(results.getTotalWordCount()).append(",\n");
        json.append("\t\"averageSentiment\": ").append(results.getAverageSentiment()).append(",\n");
        json.append("\t\"topWords\": {\n");
        
        boolean first = true;
        for (Map.Entry<String, Integer> entry : results.getTopWords().entrySet()) {
            if (!first){
                json.append(",\n");
            }
            json.append("\t\t\"").append(entry.getKey()).append("\": ").append(entry.getValue());
            first = false;
        }
        
        json.append("\n\t},\n");
        json.append("\t\"sortedSentencesCount\": ").append(results.getSortedSentences().size()).append("\n");
        json.append("}\n");
        
        try (FileWriter writer = new FileWriter(filename)) {
            writer.write(json.toString());
        }
        
        System.out.println("JSON report saved: " + filename);
    }
    
    private static void saveTextReport(KafkaAggregator.AggregatedResults results, String filename) throws IOException {
        StringBuilder report = new StringBuilder();
        
        report.append("1. WORD COUNT\n");
        report.append("Total words: ").append(results.getTotalWordCount()).append("\n\n");
        
        report.append("2. TOP WORDS\n");
        int rank = 1;
        for (Map.Entry<String, Integer> entry : results.getTopWords().entrySet()) {
            report.append(String.format("%2d. %-30s : %d occurrences\n", 
                rank++, entry.getKey(), entry.getValue()));
        }
        report.append("\n");
        
        report.append("3. SENTIMENT ANALYSIS\n");
        report.append("Average sentiment score: ").append(String.format("%.4f", results.getAverageSentiment())).append("\n");
        String sentimentLabel = results.getAverageSentiment() > 0.1 ? "POSITIVE" :
                               results.getAverageSentiment() < -0.1 ? "NEGATIVE" : "NEUTRAL";
        report.append("Sentiment label: ").append(sentimentLabel).append("\n\n");
        
        report.append("4. TEXT PROCESSING SUMMARY\n");
        report.append("Processed text length: ").append(
            results.getProcessedText() != null ? results.getProcessedText().length() : 0
        ).append(" characters\n");
        report.append("Sorted sentences count: ").append(results.getSortedSentences().size()).append("\n");
        
        Files.write(Paths.get(filename), report.toString().getBytes());
        System.out.println("Text report saved: " + filename);
    }
    
    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
