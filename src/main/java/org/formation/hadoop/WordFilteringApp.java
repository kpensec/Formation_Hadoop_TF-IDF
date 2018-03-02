package org.formation.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordFilteringApp {

    private static void usage() {
        System.out.println("<inputFolder> <outputFile>");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        try {
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FileSystem.newInstance(configuration);
            Path inputFilePath = new Path(args[0]);
            Path outputFilePath = new Path(args[1]);

            if(fileSystem.exists(outputFilePath)) {
                fileSystem.delete(outputFilePath);
            }

            Job job = new Job(configuration);
            job.setJobName("Filter");

            FileInputFormat.addInputPath(job, inputFilePath);
            FileOutputFormat.setOutputPath(job, outputFilePath);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);

            //job.setNumReduceTasks(1);
            job.setMapperClass(WordFilter.class);
            job.setReducerClass(Reducer.class);
            job.setJarByClass(WordFilteringApp.class);





            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
