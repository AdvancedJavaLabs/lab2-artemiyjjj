package org.itmo;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TextProcessor {
    private static final Set<String> POSITIVE_WORDS = new HashSet<>(Arrays.asList(
        "хорошо", "отлично", "прекрасно", "замечательно", "радость", "счастье", 
        "любовь", "дружба", "успех", "победа", "праздник", "красота", "мир",
        "good", "great", "excellent", "wonderful", "beautiful", "happy", "joy",
        "love", "success", "victory", "peace", "friend", "kind", "nice"
    ));
    
    private static final Set<String> NEGATIVE_WORDS = new HashSet<>(Arrays.asList(
        "плохо", "ужасно", "грустно", "горе", "боль", "страх", "война", "зло",
        "ненависть", "поражение", "беда", "проблема", "трудность", "страдание",
        "bad", "terrible", "awful", "sad", "pain", "fear", "war", "evil",
        "hate", "defeat", "trouble", "problem", "difficulty", "suffering"
    ));
    
    private static final Pattern NAME_PATTERN = Pattern.compile(
        "\\b[A-Z][a-z]+\\b"
    );

    public static long countWords(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0;
        }
        String[] words = text.trim().split("\\s+");
        return Arrays.stream(words)
            .filter(word -> !word.isEmpty())
            .count();
    }
    
    public static Map<String, Integer> findTopNWords(String text, int n) {
        if (text == null || text.trim().isEmpty()) {
            return new HashMap<>();
        }
        String normalized = text.toLowerCase()
            .replaceAll("[^\\p{L}\\p{N}\\s]", " ")
            .trim();
        String[] words = normalized.split("\\s+");
        Map<String, Integer> wordCount = new HashMap<>();
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty() && word.length() > 2) {
                wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
            }
        }
        return wordCount.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .limit(n)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1,
                LinkedHashMap::new
            ));
    }
    
    public static double analyzeSentiment(String text) {
        if (text == null || text.trim().isEmpty()) {
            return 0.0;
        }
        String normalized = text.toLowerCase()
            .replaceAll("[^\\p{L}\\s]", " ")
            .trim();
        String[] words = normalized.split("\\s+");
        
        int positiveCount = 0;
        int negativeCount = 0;
        int totalWords = 0;
        
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                totalWords++;
                if (POSITIVE_WORDS.contains(word)) {
                    positiveCount++;
                } else if (NEGATIVE_WORDS.contains(word)) {
                    negativeCount++;
                }
            }
        }
        if (totalWords == 0) {
            return 0.0;
        }
        
        return (double)(positiveCount - negativeCount) / Math.max(totalWords, 1);
    }
    
    public static String replaceNames(String text, String replacement) {
        if (text == null || text.trim().isEmpty()) {
            return text;
        }
        
        Matcher matcher = NAME_PATTERN.matcher(text);
        String result = matcher.replaceAll(replacement);
        
        return result;
    }
    
    public static List<String> sortSentencesByLength(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new ArrayList<>();
        }
        
        String[] sentences = text.split("[.!?]+\\s*");
        List<String> sentenceList = Arrays.stream(sentences)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
        sentenceList.sort(Comparator.comparingInt(String::length));
        return sentenceList;
    }
}

