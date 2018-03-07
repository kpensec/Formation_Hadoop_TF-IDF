package org.formation.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValue implements Writable {
    private IntWritable wordCount;
    private IntWritable wordPerDoc;

    public CustomValue() {
        this.wordCount = new IntWritable(0);
        this.wordPerDoc = new IntWritable(0);
    }

    public CustomValue(int wordCount, int wordPerDoc) {
        this.wordCount = new IntWritable(wordCount);
        this.wordPerDoc = new IntWritable(wordPerDoc);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        wordCount.write(dataOutput);
        wordPerDoc.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wordCount.readFields(dataInput);
        wordPerDoc.readFields(dataInput);
    }

    public void setWordCount(int wordCount) {
        this.wordCount.set(wordCount);
    }

    public void setWordPerDoc(int wordPerDoc) {
        this.wordPerDoc.set(wordPerDoc);
    }

    public void set(int wordCount, int wordPerDoc) {
        this.wordCount.set(wordCount);
        this.wordPerDoc.set(wordPerDoc);
    }

    public IntWritable getWordCount() {
        return wordCount;
    }

    public IntWritable getWordPerDoc() {
        return wordPerDoc;
    }

    public String toString() {
        return wordCount + " " + wordPerDoc;
    }
}
