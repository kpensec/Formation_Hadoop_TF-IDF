package org.formation.hadoop.jobs.computeTFIDF;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.formation.hadoop.CustomKey;
import org.formation.hadoop.CustomValue;

import java.io.IOException;
import java.util.LinkedHashMap;

public class Reduce extends Reducer<CustomKey, CustomValue, CustomKey, DoubleWritable> {

    private DoubleWritable outValue = new DoubleWritable();
    private CustomKey outKey = new CustomKey();


    // TODO set from main!!!
    private static int DOC_NUMBER = 2;

    protected double getTF_IDF(int tf_td, int n_d, int df_t) {
        return ((double)(tf_td) / (double)(n_d)) * Math.log(DOC_NUMBER / (double) df_t);
    }

    private class WordStats {
        public int wordPerDoc;
        public int wordCount;
    };

    @Override
    protected void reduce(CustomKey key, Iterable<CustomValue> values, Context context) throws IOException, InterruptedException {
        int occurencePerDoc = 0;

        java.util.Map<String, WordStats> wordCountMap = new LinkedHashMap();
        for (CustomValue value : values) {
            occurencePerDoc += 1;
            WordStats stats = new WordStats();
            stats.wordPerDoc = value.getWordPerDoc().get();
            stats.wordCount = value.getWordCount().get();
            wordCountMap.put(key.getFileName(), stats);
        }

        for (java.util.Map.Entry<String, WordStats> entry : wordCountMap.entrySet()) {
            outKey.set(entry.getKey(), key.getWord());
            double tfidf = getTF_IDF(entry.getValue().wordCount, entry.getValue().wordPerDoc, occurencePerDoc);

            outKey.set(entry.getKey(), key.getWord());
            outValue.set(tfidf);
            context.write(outKey, outValue);
        }
    }
}
