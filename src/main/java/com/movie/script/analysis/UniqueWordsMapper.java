package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text uniqueWords = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming each line is a dialogue line: "Character: Dialogue"
        String line = value.toString();
        String[] parts = line.split(":");
        
        if (parts.length > 1) {
            String character = parts[0].trim();
            String dialogue = parts[1].trim();
            
            Set<String> uniqueWordSet = new HashSet<>();
            
            // Extract unique words from the dialogue
            String[] words = dialogue.split("\\s+");
            for (String word : words) {
                // Clean and convert to lowercase to ignore case sensitivity and remove punctuation
                String cleanedWord = word.replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!cleanedWord.isEmpty()) {
                    uniqueWordSet.add(cleanedWord);  // Add the cleaned word
                }
            }
            
            // Join all unique words into a single string with space as separator and write to context
            uniqueWords.set(String.join(" ", uniqueWordSet));
            context.write(new Text(character), uniqueWords);
        }
    }
}