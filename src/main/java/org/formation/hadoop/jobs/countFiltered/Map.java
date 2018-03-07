package org.formation.hadoop.jobs.countFiltered;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.formation.hadoop.CustomKey;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.Set;


public class Map extends Mapper<Object, Text, CustomKey, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private CustomKey outputKey = new CustomKey();
    private Set<String> stopWords = new HashSet<String>();

    public void setup(Context context) throws IOException, InterruptedException {
        Path[] cachedFilePaths = context.getLocalCacheFiles();
        if(cachedFilePaths.length < 1) {
            throw new IOException("No cached file passed!");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(cachedFilePaths[0].toString()))) {
            String line;
            while ((line = br.readLine())!=null) {
                stopWords.add(line);
            }
        }
    }

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String alphaLine = line.toString().replaceAll("[^A-Za-z ]","");
        for (String wordPreFiltering : alphaLine.split(" ")) {
            if( wordPreFiltering.length() >= 3) {
                String wordLowered = wordPreFiltering.toLowerCase();
                if (!stopWords.contains(wordLowered)) {
                    outputKey.set(fileName, wordLowered);
                    context.write(outputKey, ONE);
                }
            }
        }


        // outputKey.set(fileName, word);
    }
}
