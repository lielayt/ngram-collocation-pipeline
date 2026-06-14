package org.example;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.*;

public class App {


    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static AmazonElasticMapReduce emr;

    private static final String REGION = envOrDefault("AWS_REGION", "us-east-1");
    private static final String JAR_BUCKET = requireEnv("JAR_BUCKET");
    private static final String PIPELINE_JAR_KEY = envOrDefault("PIPELINE_JAR_KEY", "ngram-collocation-pipeline.jar");
    private static final String PIPELINE_JAR_URI = s3Uri(JAR_BUCKET, PIPELINE_JAR_KEY);
    private static final String OUTPUT_BUCKET = requireEnv("OUTPUT_BUCKET");
    private static final String LOGS_BUCKET = requireEnv("LOGS_BUCKET");
    private static final String STOP_WORDS_URI = requireEnv("STOP_WORDS_URI");
    private static final String OUTPUT_PREFIX = envOrDefault("OUTPUT_PREFIX", "");
    private static final String INSTANCE_TYPE = envOrDefault("EMR_INSTANCE_TYPE", "m4.large");
    private static final String RELEASE_LABEL = envOrDefault("EMR_RELEASE_LABEL", "emr-5.11.0");
    private static final String SERVICE_ROLE = envOrDefault("EMR_SERVICE_ROLE", "EMR_DefaultRole");
    private static final String JOB_FLOW_ROLE = envOrDefault("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole");
    private static final String AVAILABILITY_ZONE = envOrDefault("EMR_AVAILABILITY_ZONE", REGION + "a");
    private static final String EC2_KEY_NAME = envOrDefault("EMR_EC2_KEY_NAME", "");
    private static final int NUMBER_OF_INSTANCES = intEnvOrDefault("EMR_INSTANCE_COUNT", 4);
    private static final String HEBREW_UNIGRAM_URI = envOrDefault(
            "HEBREW_UNIGRAM_URI",
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data");
    private static final String HEBREW_BIGRAM_URI = envOrDefault(
            "HEBREW_BIGRAM_URI",
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data");
    private static final String ENGLISH_UNIGRAM_URI = envOrDefault(
            "ENGLISH_UNIGRAM_URI",
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data");
    private static final String ENGLISH_BIGRAM_URI = envOrDefault(
            "ENGLISH_BIGRAM_URI",
            "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data");

    public static void main(String[]args) throws InterruptedException, IOException {


        System.out.println("[INFO] Connecting to aws");
        ec2 = AmazonEC2ClientBuilder.standard()
                .withRegion(REGION)
                .build();
        S3 = AmazonS3ClientBuilder.standard()
                .withRegion(REGION)
                .build();
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(REGION)
                .build();
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());

        //steps

        //step1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar(PIPELINE_JAR_URI)
                .withMainClass("org.example.Step1")
                .withArgs(HEBREW_UNIGRAM_URI,HEBREW_BIGRAM_URI,ENGLISH_UNIGRAM_URI,ENGLISH_BIGRAM_URI,outputUri("step1/"),STOP_WORDS_URI);

        //step1 only hebrew
//        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
//                .withJar(PIPELINE_JAR_URI)
//                .withMainClass("org.example.Step1")
//                .withArgs(HEBREW_UNIGRAM_URI,HEBREW_BIGRAM_URI,outputUri("step1/"),STOP_WORDS_URI);

        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //step2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar(PIPELINE_JAR_URI)
                .withMainClass("org.example.Step2")
                .withArgs(outputUri("step1/"),outputUri("step2/"));

        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //step3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar(PIPELINE_JAR_URI)
                .withMainClass("org.example.Step3")
                .withArgs(outputUri("step1/"),outputUri("step2/"),outputUri("step3/"));


        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //step4
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar(PIPELINE_JAR_URI)
                .withMainClass("org.example.Step4")
                .withArgs(outputUri("step3/"),outputUri("step4/"));

        StepConfig stepConfig4 = new StepConfig()
                .withName("step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUMBER_OF_INSTANCES)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion("2.9.2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(AVAILABILITY_ZONE));

        if (!EC2_KEY_NAME.isEmpty()) {
            instances.withEc2KeyName(EC2_KEY_NAME);
        }

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Ngram Collocation Pipeline")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3,stepConfig4)
                .withLogUri(s3Uri(LOGS_BUCKET, "logs/"))
                .withServiceRole(SERVICE_ROLE)
                .withJobFlowRole(JOB_FLOW_ROLE)
                .withReleaseLabel(RELEASE_LABEL);

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        System.out.println("Waiting for job flow to finish...");

        while(true) {
            DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId((jobFlowId));
            ClusterState state = ClusterState.fromValue(emr.describeCluster((describeClusterRequest)).getCluster().getStatus().getState());
            System.out.println("Cluster state: " + state);
            if (state == ClusterState.TERMINATED) {
                System.out.println("Job completed successfully");
                organizeFiles();
                break;
            } else if (state == ClusterState.TERMINATED_WITH_ERRORS) {
                System.out.println("Job failed with errors");
                break;
            }
            Thread.sleep(30000);

        }

    }

    private static class Collocation{
        String w1;
        String w2;
        double LLR;

        private Collocation(String w1, String w2, double LLR) {
            this.w1 = w1;
            this.w2 = w2;
            this.LLR = LLR;
        }
    }

    private static void organizeFiles() throws IOException {

        String bucket = OUTPUT_BUCKET;
        String prefix = outputKey("step4/");
        String hebDest = "heb_collocations.txt";
        String engDest = "eng_collocations.txt";

        Map<Integer,List<Collocation>> hebByDecade = new LinkedHashMap<Integer, List<Collocation>>();
        Map<Integer,List<Collocation>> engByDecade = new LinkedHashMap<Integer, List<Collocation>>();

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(REGION).build();

        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result res;

        do{
          res = s3.listObjectsV2(req);
          for(S3ObjectSummary summary : res.getObjectSummaries()){
              String key = summary.getKey();
              if (!key.contains("part-r"))
                  continue;

              S3Object object = s3.getObject(bucket, key);
              BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));

              String line;

              while((line = reader.readLine())!=null){

                  String[] parts = line.split("\t");
                  if (parts.length != 4)
                      continue;

                  int decade = Integer.parseInt(parts[0]);
                  String w1 = parts[1];
                  String w2 = parts[2];
                  double LLR = Double.parseDouble(parts[3]);

                  Collocation col = new Collocation(w1, w2, LLR);
                  String lang = "eng";
                  if (isHeb(w1) || isHeb(w2)) {
                      lang = "heb";
                      hebByDecade.computeIfAbsent(decade, k -> new ArrayList<Collocation>()).add(col);
                  }
                  else
                      engByDecade.computeIfAbsent(decade, k -> new ArrayList<Collocation>()).add(col);




              }
          }
        } while(res.isTruncated());

        createFile(hebByDecade,hebDest);
        createFile(engByDecade,engDest);


    }



    private static void createFile(Map<Integer,List<Collocation>> map, String dest) throws IOException {

        StringBuilder out = new StringBuilder();
        List<Integer> decades = new ArrayList<>(map.keySet());
        Collections.sort(decades);

        for (int decade : decades) {
            List<Collocation> list = map.get(decade);
            list.sort((c1, c2) -> Double.compare(c1.LLR, c2.LLR));
            out.append("Decade:  ").append(decade).append("\n");
            for (Collocation col : list) {
                out.append(col.w1).append("\t").append(col.w2).append("\t").append(col.LLR).append("\n");
            }
            out.append("\n");
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(dest))) {
            bw.write(out.toString());
            System.out.println("Created file: " + dest);
        }

    }

    private static boolean isHeb(String word){
        for (char c : word.toCharArray()) {
            if (c >= 0x0590 && c <= 0x05FF)
                return true;
        }
        return false;
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value.trim();
    }

    private static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    private static int intEnvOrDefault(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }

    private static String outputUri(String key) {
        return s3Uri(OUTPUT_BUCKET, outputKey(key));
    }

    private static String outputKey(String key) {
        String prefix = trimSlashes(OUTPUT_PREFIX);
        String cleanKey = trimLeadingSlashes(key);
        if (prefix.isEmpty()) {
            return cleanKey;
        }
        return prefix + "/" + cleanKey;
    }

    private static String s3Uri(String bucket, String key) {
        return "s3://" + bucket + "/" + trimLeadingSlashes(key);
    }

    private static String trimSlashes(String value) {
        return trimTrailingSlashes(trimLeadingSlashes(value));
    }

    private static String trimLeadingSlashes(String value) {
        String result = value == null ? "" : value.trim();
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }

    private static String trimTrailingSlashes(String value) {
        String result = value == null ? "" : value.trim();
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }


}
