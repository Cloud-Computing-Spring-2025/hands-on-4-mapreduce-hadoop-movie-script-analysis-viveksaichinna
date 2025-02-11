package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> allUniqueWords = new HashSet<>();
        
        // Merge all the unique words for each character
        for (Text val : values) {
            String[] words = val.toString().split("\\s+");
            for (String word : words) {
                allUniqueWords.add(word);
            }
        }
        
        result.set(String.join(" ", allUniqueWords));
        context.write(key, result);
    }
}