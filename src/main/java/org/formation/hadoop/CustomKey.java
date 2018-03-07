package org.formation.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {
    private Text fileName;
    private Text word;

    CustomKey() {
        this.fileName =  new Text();
        this.word =  new Text();
    }

    CustomKey(String fileName, String word) {
        this.fileName = new Text(fileName);
        this.word = new Text(word);
    }

    @Override
    public int compareTo(CustomKey rhs) {
        int result = this.fileName.compareTo(rhs.fileName);
        if(result == 0) {
            result = this.word.compareTo(rhs.word);
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

    @Override
    public String toString() {
        return fileName.toString() + ", " + word.toString();
    }
}
