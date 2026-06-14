package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class Step1 {

    // Helper to determine language and validity
    private static boolean isValid(String word) {
        if (word == null || word.isEmpty()) return false;

        boolean hasHebrew = false;
        boolean hasEnglish = false;

        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (c >= '\u0590' && c <= '\u05FF') {
                hasHebrew = true;
            } else if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
                hasEnglish = true;
            }
        }

        return (hasHebrew && !hasEnglish) || (hasEnglish && !hasHebrew);
    }


    private static String getDecade(int year) {
        return (year / 10) * 10 + "";
    }

    private static Set<String> loadStopWords(Mapper.Context context) throws IOException {
        Set<String> stops = new HashSet<>();
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {

            Path path = new Path(cacheFiles[0].getPath());
            String fileName = path.getName();

            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stops.add(line.trim().toLowerCase());
                }
            } catch (Exception e) {
                System.err.println("Error reading stop words file: " + e.getMessage());
            }
        }
        return stops;
    }

    public static class UnigramMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Set<String> stopWords = new HashSet<>();
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException {
            this.stopWords = loadStopWords(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;

            String word = parts[0];
            int year = Integer.parseInt(parts[1]);
            long count = Long.parseLong(parts[2]);
            String decade = getDecade(year);

            if (isValid(word) && !stopWords.contains(word.toLowerCase())) {
                outKey.set(decade + "\t" + word);
                outValue.set(count);
                context.write(outKey, outValue);
                outKey.set(decade + "\t*");
                context.write(outKey, outValue);
            }
        }
    }

    public static class BigramMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Set<String> stopWords = new HashSet<>();
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException {
            this.stopWords = loadStopWords(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;

            String[] words = parts[0].split(" ");
            if (words.length != 2) return;

            String w1 = words[0];
            String w2 = words[1];
            int year = Integer.parseInt(parts[1]);
            long count = Long.parseLong(parts[2]);
            String decade = getDecade(year);

            if (isValid(w1) && isValid(w2) &&
                    !stopWords.contains(w1.toLowerCase()) &&
                    !stopWords.contains(w2.toLowerCase())) {
                outKey.set(decade + "\t" + w1 + " " + w2);
                outValue.set(count);
                context.write(outKey, outValue);
            }
        }
    }

    public static class Step1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) sum += val.get();
            context.write(key, new LongWritable(sum));
        }
    }

    public static class DecadePartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decade = key.toString().split("\t")[0];
            return Math.abs(decade.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Running Step 1 with Distributed Cache");
        if (args.length < 6) {
            throw new IllegalArgumentException("Usage: Step1 <hebrew-unigram> <hebrew-bigram> <english-unigram> <english-bigram> <output> <stop-words-uri>");
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1: Count and Filter");
        job.setJarByClass(Step1.class);

        job.addCacheFile(new URI(args[5]));

        System.out.println("hebrew unigram input: " + args[0]);
        System.out.println("hebrew bigram input: " + args[1]);
        System.out.println("english unigram input: " + args[2]);
        System.out.println("english bigram input: " + args[3]);
        System.out.println("output path: " + args[4]);
        System.out.println("stop words URI: " + args[5]);

        MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, UnigramMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), SequenceFileInputFormat.class, UnigramMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, BigramMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), SequenceFileInputFormat.class, BigramMapper.class);

        // Only hebrew Ngrams
//        System.out.println("hebrew unigram input: " + args[0]);
//        System.out.println("hebrew bigram input: " + args[1]);
//        System.out.println("output path: " + args[2]);
//
//        MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, UnigramMapper.class);
//
//        MultipleInputs.addInputPath(job, new Path(args[1]), SequenceFileInputFormat.class, BigramMapper.class);


        job.setReducerClass(Step1Reducer.class);

        job.setCombinerClass(Step1Reducer.class);

        job.setPartitionerClass(DecadePartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
