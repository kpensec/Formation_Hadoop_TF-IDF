package org.formation.hadoop.jobs.wordPerDoc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.formation.hadoop.CustomKey;
import org.formation.hadoop.CustomValue;

import java.io.IOException;
import java.util.LinkedHashMap;

public class Reduce extends Reducer<CustomKey, CustomValue, CustomKey, CustomValue> {

    private CustomValue outValue = new CustomValue();
    private CustomKey outKey = new CustomKey();

    @Override
    protected void reduce(CustomKey key, Iterable<CustomValue> values, Context context) throws IOException, InterruptedException {
        int total = 0;

        java.util.Map<String, Integer> wordCountMap = new LinkedHashMap();
        for (CustomValue value : values) {
            total += value.getWordCount().get();
            wordCountMap.put(key.getWord(), value.getWordCount().get());
        }

        for ( java.util.Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
            outKey = new CustomKey(key.getFileName(), entry.getKey());
            outValue = new CustomValue(entry.getValue(), total);
            context.write(outKey, outValue);
        }
    }
}
