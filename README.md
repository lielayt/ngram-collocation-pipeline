# Ngram Collocation Pipeline

This project runs a Hadoop MapReduce pipeline on Amazon EMR to find high-scoring
word collocations by decade for English and Hebrew Google Books ngram datasets.

The pipeline has four stages:

1. Count unigram, bigram, and decade totals after filtering invalid tokens and stop words.
2. Join each bigram count with the count of its first word.
3. Join with the count of the second word and calculate the log-likelihood ratio.
4. Keep the top 100 collocations for each decade and language.

## Requirements

- Java 8
- Maven
- AWS credentials available to the default AWS SDK credential provider chain
- An S3 bucket containing the Maven-built pipeline JAR
- An S3 bucket for EMR output
- An S3 bucket for EMR logs
- A stop-words file in S3

## Configuration

The local driver reads AWS and EMR settings from environment variables:

| Variable | Required | Description |
| --- | --- | --- |
| `JAR_BUCKET` | Yes | S3 bucket containing the Maven-built pipeline JAR. |
| `OUTPUT_BUCKET` | Yes | S3 bucket where pipeline outputs are written. |
| `LOGS_BUCKET` | Yes | S3 bucket for EMR logs. |
| `STOP_WORDS_URI` | Yes | Full S3 URI for the stop-words file. |
| `AWS_REGION` | No | AWS region. Defaults to `us-east-1`. |
| `PIPELINE_JAR_KEY` | No | Object key for the pipeline JAR in `JAR_BUCKET`. Defaults to `ngram-collocation-pipeline.jar`. |
| `OUTPUT_PREFIX` | No | Prefix inside `OUTPUT_BUCKET`. Defaults to the bucket root. |
| `EMR_EC2_KEY_NAME` | No | EC2 key pair name for the cluster. |
| `EMR_SERVICE_ROLE` | No | EMR service role. Defaults to `EMR_DefaultRole`. |
| `EMR_JOB_FLOW_ROLE` | No | EMR EC2 instance profile. Defaults to `EMR_EC2_DefaultRole`. |
| `EMR_INSTANCE_COUNT` | No | Number of EMR instances. Defaults to `4`. |
| `EMR_INSTANCE_TYPE` | No | Master and core instance type. Defaults to `m4.large`. |
| `EMR_RELEASE_LABEL` | No | EMR release label. Defaults to `emr-5.11.0`. |
| `EMR_AVAILABILITY_ZONE` | No | Cluster availability zone. Defaults to `<AWS_REGION>a`. |

## Build

```bash
mvn package
```

This creates:

```text
target/ngram-collocation-pipeline.jar
```

Upload that JAR to the configured `JAR_BUCKET` before running the driver:

```bash
aws s3 cp target/ngram-collocation-pipeline.jar s3://my-ngram-jars/ngram-collocation-pipeline.jar
```

## Run

```bash
export AWS_REGION=us-east-1
export JAR_BUCKET=my-ngram-jars
export OUTPUT_BUCKET=my-ngram-output
export LOGS_BUCKET=my-ngram-logs
export STOP_WORDS_URI=s3://my-ngram-config/eng-stopwords.txt

java -jar target/ngram-collocation-pipeline.jar
```

When the EMR cluster terminates successfully, the driver downloads the final
small outputs and writes:

- `eng_collocations.txt`
- `heb_collocations.txt`

These output files are generated artifacts and are intentionally ignored by git.
