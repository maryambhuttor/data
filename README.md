# Multi-Format Research Data QA (Hadoop/Java) — Setup & Run Guide

## Overview
A Hadoop MapReduce job in Java that ingests three research metadata CSVs — imaging (.dcm listed in CSV), voice (.wav listed in CSV), and app logs — and emits QA counters:
- Totals per data type
- Missing/invalid field checks
- Local duplicate detection (per mapper split) by PatientID|FileName
- Per-scan-type counts (MRI/PET/CT)

## Inspired by (Homework lineage)
This project’s structure (Driver + Mapper + Reducer, line-oriented parsing, map emits (key,1) → reduce sums) was inspired by my school assignment `Problem4.java`. I extended the idea to:
- Parse **multiple file schemas** by detecting the input file (FileSplit) and routing logic per schema
- Add **richer QA rules** (missing vs. invalid numeric, resolution format)
- Introduce **local duplicate detection** by composite key
- Emit **analysis-ready metric keys** (e.g., IMAGE_BY_SCANTYPE|MRI)

## 1) Download & Install

### Java 11+ (JDK)
- Ubuntu / Codespaces:
    sudo apt-get update -y
    sudo apt-get install -y openjdk-11-jdk wget tar

- macOS:
    brew install openjdk@11
    # Follow brew’s output to set JAVA_HOME if needed

- Windows:
    Install Adoptium Temurin JDK 11 (or OpenJDK/Oracle JDK 11)

### Hadoop 3.x (tested with 3.3.6)
- Download & unpack:
    wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    tar -xzf hadoop-3.3.6.tar.gz

- Set environment (current shell):
    export HADOOP_HOME="$PWD/hadoop-3.3.6"
    export PATH="$HADOOP_HOME/bin:$PATH"

- Sanity checks:
    java -version
    hadoop version

(Tip: add the two export lines to ~/.bashrc to persist.)

## 2) Project Structure
data/   ← sample CSV inputs
src/    ← Java source files (Mapper, Reducer, Driver or single-file job)

Expected files:
- data/images_metadata.csv
- data/voice_metadata.csv
- data/app_log.csv
- src/*.java  (e.g., MultiFormatDriver.java, QA_Mapper.java, QA_Reducer.java — or a single MultiFormatQAEnhanced.java)

## 3) Compile
mkdir -p classes
javac -cp "`hadoop classpath`" -d classes src/*.java

## 4) Package
jar -cvf pipeline.jar -C classes/ .

## 5) Run
# remove old output directory if present
rm -rf output

# run (input dir = data/, output dir = output/)
hadoop jar pipeline.jar MultiFormatDriver data output

# view results
cat output/part-r-00000 | sort

## 6) Expected Output (with the provided sample data)
CSV_MISSING_VALUE       1
CSV_TOTAL               4
DUPLICATE_LOCAL_IMAGE   1
DUPLICATE_LOCAL_TOTAL   2
DUPLICATE_LOCAL_VOICE   1
IMAGE_BY_SCANTYPE|CT    1
IMAGE_BY_SCANTYPE|MRI   3
IMAGE_BY_SCANTYPE|PET   1
IMAGE_MISSING_FILESIZE  1
IMAGE_MISSING_RESOLUTION        1
IMAGE_TOTAL             5
VOICE_MISSING_DURATION  1
VOICE_MISSING_SAMPLERATE        1
VOICE_TOTAL             4

## 7) Why Hadoop/Java (vs. Python/Pandas)
**When this is better than Pandas:**
- **Scale & parallelism:** Hadoop executes your mapper/reducer across many cores/machines. Pandas is single-node and RAM-bound; large datasets can exhaust memory.
- **Fault tolerance:** Hadoop re-runs failed tasks automatically; great for long-running or large jobs.
- **Data locality:** Moves compute to where data lives (HDFS/S3), reducing network bottlenecks.
- **Deterministic batch & counters:** MapReduce job counters and emitted keys produce clean, auditable aggregates (nice for research QA/provenance).
- **Schema-aware branching:** FileSplit-based routing lets one job process heterogeneous inputs cleanly at scale.

**When Pandas might be better:**
- **Small-to-medium data** that fits in memory
- **Rapid prototyping/EDA** with rich Python ecosystem (NumPy/Matplotlib/Polars)
- **Interactive development** where iteration speed matters more than distributed throughput

**Bottom line:** Pandas is fantastic for exploratory/medium workloads. Hadoop/Java demonstrates production-grade, distributed processing competence and comfortably scales to very large, multi-format datasets with clear QA/provenance steps.

## 8) Troubleshooting
- `hadoop: command not found` → Re-export HADOOP_HOME and PATH as above.
- `*.class : no such file or directory` when creating JAR → Run the **compile** step first (classes/ must contain .class files).
- `File … already exists` on run → Remove previous output dir (`rm -rf output`).
- Wrong metrics for voice/images → Ensure the **filenames** inside `data/` match the expected schemas:
  - `images_metadata.csv`: PatientID,ScanType,FileName,FileSize,Resolution
  - `voice_metadata.csv` : PatientID,FileName,Duration_sec,SampleRate
  - `app_log.csv`        : PatientID,Timestamp,EventType,Value
