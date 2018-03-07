package org.formation.hadoop.jobs.countFiltered;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.formation.hadoop.CustomKey;


public class Reduce extends Reducer<CustomKey, IntWritable, CustomKey, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(CustomKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }

}