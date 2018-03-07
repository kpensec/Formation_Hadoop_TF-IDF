package org.formation.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {
    private Text fileName;
    private Text word;

    public CustomKey() {
        this.fileName =  new Text();
        this.word =  new Text();
    }

    public CustomKey(String fileName, String word) {
        this.fileName = new Text(fileName);
        this.word = new Text(word);
    }

    @Override
    public int compareTo(CustomKey rhs) {
        int result = this.word.compareTo(rhs.word);
        if(result == 0) {
            result = this.fileName.compareTo(rhs.fileName);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        fileName.write(dataOutput);
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileName.readFields(dataInput);
        word.readFields(dataInput);
    }

    public void set(String fileName, String word) {
        this.fileName.set(fileName);
        this.word.set(word);
    }

    public void set(CustomKey lhs) {
        this.fileName = lhs.fileName;
        this.word = lhs.word;
    }

    @Override
    public String toString() {
        return fileName.toString() + " " + word.toString();
    }

    public String getFileName() {
        return fileName.toString();
    }

    public String getWord() {
        return word.toString();
    }
}
