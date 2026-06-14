# Ngram Collocation Pipeline

This project runs a Hadoop MapReduce pipeline on Amazon EMR to find high-scoring
word collocations by decade for English and Hebrew Google Books ngram datasets.

A collocation is an ordered word pair that appears together more often than
would be expected if the two words occurred independently. Examples include
phrases such as `New York`, `human beings`, or domain-specific Hebrew word
pairs that recur across books from the same decade.

## Scoring Model

The project scores each ordered bigram `(w1, w2)` with a log-likelihood ratio.
For each decade, the pipeline computes:

| Symbol | Meaning |
| --- | --- |
| `c1` | Count of `w1`. |
| `c2` | Count of `w2`. |
| `c12` | Count of the ordered bigram `w1 w2`. |
| `N` | Total unigram count for the decade. |

The null model assumes `w2` follows `w1` with the same probability as it appears
globally:

```text
p  = c2 / N
```

The alternative model estimates separate probabilities for contexts after `w1`
and not after `w1`:

```text
p1 = c12 / c1
p2 = (c2 - c12) / (N - c1)
```

For a binomial log-likelihood:

```text
logL(k, n, x) = k log(x) + (n - k) log(1 - x)
```

the score used by the reducers is:

```text
log lambda =
    logL(c12, c1, p)
  + logL(c2 - c12, N - c1, p)
  - logL(c12, c1, p1)
  - logL(c2 - c12, N - c1, p2)
```

Lower `log lambda` values indicate stronger evidence that the two words form a
collocation. This is equivalent to ranking by the positive statistic
`-2 * log lambda` in the opposite direction.

## Pipeline

The pipeline has four stages:

1. Count unigram, bigram, and decade totals after filtering invalid tokens and stop words.
2. Join each bigram count with the count of its first word.
3. Join with the count of the second word and calculate the log-likelihood ratio.
4. Keep the top 100 collocations for each decade and language.

The joins are split into separate MapReduce jobs so reducers do not need to keep
all word counts or all bigrams for a decade in memory. Step 2 groups records by
`(decade, w1)` so `c1` reaches the reducer before matching bigrams. Step 3 uses
the same pattern for `(decade, w2)` and loads only decade totals from Step 1.

## Data

By default, the driver reads from the public Google Books ngram datasets hosted
for EMR:

| Language | Unigrams | Bigrams |
| --- | --- | --- |
| English | `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data` | `s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data` |
| Hebrew | `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data` | `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data` |

The ngram files are Hadoop sequence files. Step 1 reads them with
`SequenceFileInputFormat`.

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
| `ENGLISH_UNIGRAM_URI` | No | English unigram dataset URI. |
| `ENGLISH_BIGRAM_URI` | No | English bigram dataset URI. |
| `HEBREW_UNIGRAM_URI` | No | Hebrew unigram dataset URI. |
| `HEBREW_BIGRAM_URI` | No | Hebrew bigram dataset URI. |

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

Each output file is grouped by decade:

```text
Decade:  2000
w1    w2    log_lambda
```

## Implementation Notes

- Stop words are loaded through Hadoop's distributed cache and removed before
  unigram and bigram counts are emitted.
- Step 1 uses a combiner to reduce network traffic for repeated count keys.
- Custom partitioners keep records from the same decade and join key on the same
  reducer.
- Custom grouping comparators allow unigram counts and related bigram records to
  be processed as one logical reduce group.
- The final step keeps only 100 candidates per `(decade, language)` group with a
  priority queue instead of sorting the full reducer input.

## Runtime Statistics

For large EMR runs, Hadoop counters are useful for comparing the effect of local
aggregation:

| Metric | Why it matters |
| --- | --- |
| Map input records | Raw ngram volume processed by each stage. |
| Map output records | Number of intermediate key-value pairs emitted. |
| Map output bytes | Shuffle size before compression and merge effects. |
| Combine input/output records | Effectiveness of the Step 1 combiner. |
| Reduce input records | Actual shuffle load reaching reducers. |
| Reduce output records | Size of the materialized stage output. |

These counters can be read from the EMR step logs after each run.
