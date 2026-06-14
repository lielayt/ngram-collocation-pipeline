package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class Step4 {

    private static String getLang(String w1,String w2){
        if (containsHebrew(w1) && containsHebrew(w2)) {
            return "he";
        }
        return "en";
    }

    private static boolean containsHebrew(String word) {
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (c >= '\u0590' && c <= '\u05FF') {
                return true;
            }
        }
        return false;
    }


    private static class TopHundredMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            if (parts.length < 4) {
                return;
            }

            String decade = parts[0];
            double LLR = Double.parseDouble(parts[1]);
            String w1 = parts[2];
            String w2 = parts[3];

            String lang = getLang(w1,w2);

            k.set(decade+"\t"+lang);
            v.set(LLR+"\t"+w1+"\t"+w2);
            context.write(k,v);

        }
    }

    public static class TopHundredReducer extends Reducer<Text, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        private class Bigram{
            private String w1;
            private String w2;
            private double LLR;

            private Bigram(String w1, String w2, double LLR) {
                this.w1 = w1;
                this.w2 = w2;
                this.LLR = LLR;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            PriorityQueue<Bigram> pq = new PriorityQueue<>(100, (a, b) -> Double.compare(b.LLR, a.LLR));
            String decade = key.toString().split("\t")[0];

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length < 3) {
                    continue;
                }

                double LLR = Double.parseDouble(parts[0]);
                String w1 = parts[1];
                String w2 = parts[2];
                pq.add(new Bigram(w1, w2, LLR));

                if(pq.size() > 100) {
                    pq.poll();
                }
            }

            List<Bigram> top100 = new ArrayList<>(pq);
            top100.sort(Comparator.comparingDouble(b -> b.LLR));

            for (Bigram b : top100) {
                k.set(decade);
                v.set(b.w1 + "\t" + b.w2 + "\t" + b.LLR);
                context.write(k,v);
            }
        }


    }

    private static class Top100Partitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {

            System.out.println("running step 4");
            if (args.length < 2) {
                throw new IllegalArgumentException("Usage: Step4 <step3-output> <output>");
            }

            System.out.println("input path: " + args[0]);
            System.out.println("output path: " + args[1]);


            Configuration config = new Configuration();
            Job job = Job.getInstance(config, "Word Pair Aggregation");
            job.setJarByClass(Step4.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setPartitionerClass(Top100Partitioner.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TopHundredMapper.class);
            job.setReducerClass(TopHundredReducer.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}

