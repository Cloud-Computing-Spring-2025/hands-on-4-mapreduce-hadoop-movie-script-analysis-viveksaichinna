package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalLength = 0;
        
        // Sum up the lengths of the dialogues for each character
        for (IntWritable val : values) {
            totalLength += val.get();
        }
        
        result.set(totalLength);
        context.write(key, result);
    }
}