package org.formation.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URI;

public class WordFilteringApp {

    private static void usage() {
        System.out.println("<inputFolder> <outputFile>");
    }

    private static final URI stopwordsURI = new Path("cache/TF-IDF/stopwords_en.txt").toUri();

    public static void main(String[] args) {
        int startFrom = 0, endTask = 10;
        if (args.length < 2) {
            usage();
            System.exit(1);
        }
        if (args.length > 2) {
            startFrom = Integer.parseInt(args[2]);
        }
        if (args.length > 3) {
            endTask = Integer.parseInt(args[3]);
        }

        try {
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FileSystem.newInstance(configuration);
            Path inputFilePath = new Path(args[0]);

            Path[] tmpOutputFile = {
                    new Path("tmp/TF-IDF/Job1"),
                    new Path("tmp/TF-IDF/Job2"),
                    new Path("tmp/TF-IDF/Job3"),
                    new Path("tmp/TF-IDF/Job4"),
            };
            Path outputFilePath = new Path(args[1]);

            if(fileSystem.exists(outputFilePath)) {
                fileSystem.delete(outputFilePath);
            }

            if(startFrom < 1 && endTask >= 0) { // First job (filter and word count):
                System.out.println("Starting Job 1");
                Job job = new Job(configuration);
                if(fileSystem.exists(tmpOutputFile[0])) {
                    fileSystem.delete(tmpOutputFile[0]);
                }

                job.setJobName("Filter Word Count");

                FileInputFormat.addInputPath(job, inputFilePath);
                FileOutputFormat.setOutputPath(job, tmpOutputFile[0]);

                job.setOutputKeyClass(CustomKey.class);
                job.setOutputValueClass(IntWritable.class);

                job.setMapperClass(org.formation.hadoop.jobs.countFiltered.Map.class);
                job.setReducerClass(org.formation.hadoop.jobs.countFiltered.Reduce.class);

                job.setJarByClass(WordFilteringApp.class);
                // adding cached files:
                job.addCacheFile(stopwordsURI);

                job.waitForCompletion(true);
            }

            if (startFrom < 2 && endTask >= 1) { // Seccond job (count word per doc):
                System.out.println("Starting Job 2");
                Job job = new Job(configuration);
                job.setJobName("Word Total Count");
                if(fileSystem.exists(tmpOutputFile[1])) {
                    fileSystem.delete(tmpOutputFile[1]);
                }

                FileInputFormat.addInputPath(job, tmpOutputFile[0]);
                FileOutputFormat.setOutputPath(job, tmpOutputFile[1]);

                job.setOutputKeyClass(CustomKey.class);
                job.setOutputValueClass(CustomValue.class);

                job.setMapperClass(org.formation.hadoop.jobs.wordPerDoc.Map.class);
                job.setPartitionerClass(org.formation.hadoop.jobs.wordPerDoc.Partitioner.class);
                job.setGroupingComparatorClass(org.formation.hadoop.jobs.wordPerDoc.GroupingComparator.class);
                job.setReducerClass(org.formation.hadoop.jobs.wordPerDoc.Reduce.class);

                job.setJarByClass(WordFilteringApp.class);
                job.setNumReduceTasks(1);

                job.waitForCompletion(true);
            }

            if (startFrom < 3 && endTask >= 2) { // Last job (TF-IDF):
                System.out.println("Starting Job 3");
                Job job = new Job(configuration);
                job.setJobName("TF-IDF computation");
                if(fileSystem.exists(tmpOutputFile[2])) {
                    fileSystem.delete(tmpOutputFile[2]);
                }

                FileInputFormat.addInputPath(job, tmpOutputFile[1]);
                FileOutputFormat.setOutputPath(job, tmpOutputFile[2]);

                job.setOutputKeyClass(CustomKey.class);
                job.setMapOutputValueClass(CustomValue.class);
                job.setOutputValueClass(DoubleWritable.class);

                job.setMapperClass(org.formation.hadoop.jobs.computeTFIDF.Map.class);
                job.setPartitionerClass(org.formation.hadoop.jobs.computeTFIDF.Partitioner.class);
                job.setGroupingComparatorClass(org.formation.hadoop.jobs.computeTFIDF.GroupingComparator.class);
                job.setReducerClass(org.formation.hadoop.jobs.computeTFIDF.Reduce.class);

                job.setJarByClass(WordFilteringApp.class);

                job.waitForCompletion(true);
            }

            if (startFrom < 4&& endTask >= 3){ // sorting on TF-IDF values:
                System.out.println("Starting Job 4");
                Job job = new Job(configuration);
                job.setJobName("TF-IDF sorting");

                FileInputFormat.addInputPath(job, tmpOutputFile[2]);
                FileOutputFormat.setOutputPath(job, outputFilePath);

                job.setOutputKeyClass(DoubleWritable.class);
                job.setOutputValueClass(CustomKey.class);

                job.setMapperClass(org.formation.hadoop.jobs.sort.Map.class);
                job.setJarByClass(WordFilteringApp.class);

                job.waitForCompletion(true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
