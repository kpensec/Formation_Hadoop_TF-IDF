package org.formation.hadoop.jobs.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.formation.hadoop.CustomKey;

import java.io.IOException;

public class Map extends Mapper<Object, Text, DoubleWritable,CustomKey> {
    private DoubleWritable outputKey = new DoubleWritable(0);
    private CustomKey outputValue = new CustomKey("foo","bar");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("[ \t]");
        outputValue.set(data[0], data[1]);
        outputKey.set(Double.parseDouble(data[2]));
        context.write(outputKey, outputValue);
    }
}
