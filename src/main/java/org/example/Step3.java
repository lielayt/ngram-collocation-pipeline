package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Step3 {


    public static class DecadeSecondWordKey implements WritableComparable<DecadeSecondWordKey> {
        public Text decade = new Text();
        public Text w2 = new Text();
        public int type;

        public DecadeSecondWordKey() {}
        public DecadeSecondWordKey(String d, String word, int t) {
            this.decade.set(d);
            this.w2.set(word);
            this.type = t;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            decade.write(out);
            w2.write(out);
            out.writeInt(type);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            decade.readFields(in);
            w2.readFields(in);
            type = in.readInt();
        }

        @Override
        public int compareTo(DecadeSecondWordKey other) {
            int d = this.decade.compareTo(other.decade);
            if (d != 0) return d;
            int w = this.w2.compareTo(other.w2);
            if (w != 0) return w;
            return Integer.compare(this.type, other.type);
        }
    }


    public static class Step1InputMapper extends Mapper<Object, Text, DecadeSecondWordKey, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;

            String decade = parts[0];
            String word = parts[1];
            String count = parts[2];


            if(word.split(" ").length>1)
                return;

            if (word.equals("*")) return;

            context.write(new DecadeSecondWordKey(decade, word, 1), new Text(count));
        }
    }


    public static class Step2InputMapper extends Mapper<Object, Text, DecadeSecondWordKey, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");


            if (parts.length < 5) return;

            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            String c12 = parts[3];
            String c1 = parts[4];


            context.write(new DecadeSecondWordKey(decade, w2, 2), new Text(w1 + "\t" + c12 + "\t" + c1));
        }
    }


    public static class DecadeW2Partitioner extends Partitioner<DecadeSecondWordKey, Text> {
        @Override
        public int getPartition(DecadeSecondWordKey key, Text value, int numPartitions) {
            return Math.abs((key.decade.toString() + key.w2.toString()).hashCode()) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() { super(DecadeSecondWordKey.class, true); }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DecadeSecondWordKey k1 = (DecadeSecondWordKey) a;
            DecadeSecondWordKey k2 = (DecadeSecondWordKey) b;
            int d = k1.decade.compareTo(k2.decade);
            if (d != 0) return d;
            return k1.w2.compareTo(k2.w2);
        }
    }

    public static class LLRReducer extends Reducer<DecadeSecondWordKey, Text, Text, Text> {

        private Map<String, Long> decadeTotals = new HashMap<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String step1Path = conf.get("step1.output.path");

            if (step1Path == null) {
                System.err.println("Step 1 path not set in configuration!");
                return;
            }

            Path path = new Path(step1Path);
            FileSystem fs = path.getFileSystem(conf);
            FileStatus[] files = fs.listStatus(path);

            for (FileStatus status : files) {
                if (!status.isDirectory() && status.getPath().getName().startsWith("part")) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 3 && parts[1].equals("*")) {
                            String decade = parts[0];
                            long count = Long.parseLong(parts[2]);
                            decadeTotals.put(decade, count);
                        }
                    }
                    br.close();
                }
            }
        }

        @Override
        protected void reduce(DecadeSecondWordKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String decade = key.decade.toString();
            Long N_long = decadeTotals.get(decade);
            boolean first = true;

            if (N_long == null) {
                return;
            }
            double N = N_long.doubleValue();
            double c2 = 0;

            for (Text val : values) {
                if (first) {
                    c2 = Double.parseDouble(val.toString());
                    first = false;
                } else {
                    if (c2 > 0) {
                        String[] parts = val.toString().split("\t");
                        String w1 = parts[0];
                        double c12 = Double.parseDouble(parts[1]);
                        double c1 = Double.parseDouble(parts[2]);

                        double llr = calculateLLR(c12, c1, c2, N);

                        context.write(new Text(decade), new Text(llr + "\t" + w1 + "\t" + key.w2));

                    }
                }
            }
        }

        private double calculateLLR(double c12, double c1, double c2, double N) {
            double p = c2 / N;
            double p1 = c12 / c1;
            double p2 = (c2 - c12) / (N - c1);

            // log lambda compares independence against the bigram-specific association model.
            // A more negative value means stronger evidence of collocation; -2 * log(lambda)
            // is the equivalent positive likelihood-ratio score.
            double term1 = logL(c12, c1, p);
            double term2 = logL(c2 - c12, N - c1, p);
            double term3 = logL(c12, c1, p1);
            double term4 = logL(c2 - c12, N - c1, p2);

            return term1 + term2 - term3 - term4;
        }

        private double logL(double k, double n, double x) {
            if (x == 0 || x == 1) return 0;
            return k * Math.log(x) + (n - k) * Math.log(1 - x);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Running Step 3: LLR Calculation");
        if (args.length < 3) {
            throw new IllegalArgumentException("Usage: Step3 <step1-output> <step2-output> <output>");
        }

        System.out.println("step1 input path: " + args[0]);
        System.out.println("step2 input path: " + args[1]);
        System.out.println("output path: " + args[2]);

        Configuration conf = new Configuration();


        conf.set("step1.output.path", args[0]);

        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Step3.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Step1InputMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Step2InputMapper.class);

        job.setPartitionerClass(DecadeW2Partitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(LLRReducer.class);

        job.setMapOutputKeyClass(DecadeSecondWordKey.class);
        job.setMapOutputValueClass(Text.class);

        // Final Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); // Outputting (Decade) and (LLR w1 w2)

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
