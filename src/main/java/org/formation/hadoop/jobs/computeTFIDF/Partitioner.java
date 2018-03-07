package org.formation.hadoop.jobs.computeTFIDF;

import org.formation.hadoop.CustomKey;
import org.formation.hadoop.CustomValue;

public class Partitioner extends org.apache.hadoop.mapreduce.Partitioner<CustomKey, CustomValue> {
    public int getPartition(CustomKey key, CustomValue value, int numPartitions) {
        int hash = key.getWord().hashCode();
        return hash % numPartitions;
    }
}
