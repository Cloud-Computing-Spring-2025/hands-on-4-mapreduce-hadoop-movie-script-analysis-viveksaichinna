package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming each line is a dialogue line: "Character: Dialogue"
        String line = value.toString();
        String[] parts = line.split(":");
        
        if (parts.length > 1) {
            String character = parts[0].trim();
            String dialogue = parts[1].trim();
            
            // Split the dialogue into words and write out word counts per character
            String[] words = dialogue.split("\\s+");
            for (String w : words) {
                word.set(character + ":" + w);
                context.write(word, one);
            }
        }
    }
}