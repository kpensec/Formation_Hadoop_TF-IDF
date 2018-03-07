package org.formation.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        int startFrom = 0;
        if (args.length < 2) {
            usage();
            System.exit(1);
        }
        if (args.length > 2) {
            startFrom = Integer.parseInt(args[2]);
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

            if(startFrom < 1) { // First job (filter and word count):

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

            if (startFrom < 2){ // Seccond job (count word per doc):
                Job job = new Job(configuration);
                job.setJobName("Word Total Count");

                FileInputFormat.addInputPath(job, tmpOutputFile[0]);
                FileOutputFormat.setOutputPath(job, outputFilePath);

                job.setOutputKeyClass(CustomKey.class);
                job.setOutputValueClass(CustomValue.class);

                job.setMapperClass(org.formation.hadoop.jobs.wordPerDoc.Map.class);
                job.setPartitionerClass(org.formation.hadoop.jobs.wordPerDoc.Partitioner.class);
                job.setGroupingComparatorClass(org.formation.hadoop.jobs.wordPerDoc.GroupingComparator.class);
                job.setReducerClass(org.formation.hadoop.jobs.wordPerDoc.Reduce.class);

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
