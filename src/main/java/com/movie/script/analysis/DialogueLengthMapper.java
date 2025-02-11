package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable length = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming each line is a dialogue line: "Character: Dialogue"
        String line = value.toString();
        String[] parts = line.split(":");

        if (parts.length > 1) {
            String character = parts[0].trim();
            String dialogue = parts[1].trim();
            
            // Calculate the length of the dialogue (in words)
            length.set(dialogue.split("\\s+").length);
            context.write(new Text(character), length);
        }
    }
}