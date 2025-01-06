package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Step1 {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

            String[] parts = value.toString().split("\t");

            if(parts.length != 5)
                return;


            String ngram = parts[0];
            String[] words = ngram.split(" ");
            int matchCount = Integer.parseInt(parts[2]);
            int pageCount = Integer.parseInt(parts[3]);
            int volumeCount = Integer.parseInt(parts[4]);

            //N_3
            context.write(new Text("trigram "+words[0]+words[1]+words[2]), new IntWritable(matchCount));
            //C_0
            context.write(new Text("total"), new IntWritable(matchCount * 3));
            //C_2
            context.write(new Text("bigram "+words[0]+words[1]), new IntWritable(matchCount));


        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            Text text = new Text();
            text.set(key.toString().split(" ")[1]);
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(text, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text,IntWritable> {

        @Override
        public int getPartition(Text text, IntWritable intWritable, int numOfPartitions) {

            String op = text.toString().split(" ")[0];
            switch(op){
                case "bigram":
                    return 0;
                case "trigram":
                    return 1;
                case "total":
                    return 2;
                default:
                    return 0;

            }
        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setNumReduceTasks(3);
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path("s3://bucket163897429777/arbix.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://bucket163897429777/output_word_count"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
