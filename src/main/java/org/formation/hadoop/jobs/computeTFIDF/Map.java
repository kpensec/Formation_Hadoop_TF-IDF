package org.formation.hadoop.jobs.computeTFIDF;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.formation.hadoop.CustomKey;
import org.formation.hadoop.CustomValue;

import java.io.IOException;

public class Map extends Mapper<Object, Text, CustomKey, CustomValue> {
    private CustomValue outputValue = new CustomValue(1, 1);
    private CustomKey outputKey = new CustomKey("foo","bar");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("[ \t]");
        outputKey.set(data[0], data[1]);
        outputValue.set(Integer.parseInt(data[2]), Integer.parseInt(data[3]));
        context.write(outputKey, outputValue);
    }
}