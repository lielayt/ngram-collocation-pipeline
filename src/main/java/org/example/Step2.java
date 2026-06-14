package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Step2 {


    public static class DecadeWordKey implements WritableComparable<DecadeWordKey> {
        public Text decade = new Text();
        public Text word = new Text();
        public int type; // 1 = Unigram (c1), 2 = Bigram (c12)

        public DecadeWordKey() {}
        public DecadeWordKey(String d, String w, int t) {
            this.decade.set(d);
            this.word.set(w);
            this.type = t;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            decade.write(out);
            word.write(out);
            out.writeInt(type);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            decade.readFields(in);
            word.readFields(in);
            type = in.readInt();
        }

        @Override
        public int compareTo(DecadeWordKey other) {
            int d = this.decade.compareTo(other.decade);
            if (d != 0) return d;
            int w = this.word.compareTo(other.word);
            if (w != 0) return w;
            return Integer.compare(this.type, other.type);
        }
    }


    public static class Step2Mapper extends Mapper<Object, Text, DecadeWordKey, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;

            String decade = parts[0];
            String wordField = parts[1];
            String count = parts[2];

            if (wordField.equals("*")) return;

            String[] words = wordField.split(" ");

            if (words.length == 1) {
                context.write(new DecadeWordKey(decade, words[0], 1), new Text(count));

            } else if (words.length == 2) {
                context.write(new DecadeWordKey(decade, words[0], 2), new Text(words[1] + "\t" + count));
            }
        }
    }

    public static class DecadeWordPartitioner extends Partitioner<DecadeWordKey, Text> {
        @Override
        public int getPartition(DecadeWordKey key, Text value, int numPartitions) {
            return Math.abs((key.decade.toString() + key.word.toString()).hashCode()) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(DecadeWordKey.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DecadeWordKey k1 = (DecadeWordKey) a;
            DecadeWordKey k2 = (DecadeWordKey) b;
            int d = k1.decade.compareTo(k2.decade);
            if (d != 0) return d;
            return k1.word.compareTo(k2.word);
        }
    }

    public static class Step2Reducer extends Reducer<DecadeWordKey, Text, Text, Text> {
        @Override
        protected void reduce(DecadeWordKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean first = true;
            String c1 = null;

            for (Text val : values) {
                if (first) {
                    c1 = val.toString();
                    first = false;
                } else {
                    if (c1 != null) {
                        String[] parts = val.toString().split("\t");
                        String w2 = parts[0];
                        String c12 = parts[1];

                        context.write(new Text(key.decade + "\t" + key.word + "\t" + w2),
                                new Text(c12 + "\t" + c1));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Running Step 2: Join c1...");
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: Step2 <step1-output> <output>");
        }

        System.out.println("input path: " + args[0]);
        System.out.println("output path: " + args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 2: Join c1");
        job.setJarByClass(Step2.class);

        job.setMapperClass(Step2Mapper.class);
        job.setPartitionerClass(DecadeWordPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(Step2Reducer.class);

        job.setMapOutputKeyClass(DecadeWordKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class); // Reading Text files from Step 1
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
