package org.formation.hadoop.jobs.computeTFIDF;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.formation.hadoop.CustomKey;

public class GroupingComparator extends WritableComparator {
    GroupingComparator() {
        super(CustomKey.class, true);
    }

    public int compare(WritableComparable writable1, WritableComparable writable2) {
        CustomKey key1 = (CustomKey) writable1;
        CustomKey key2 = (CustomKey) writable2;

        return key1.getWord().compareTo(key2.getWord());
    }
}
