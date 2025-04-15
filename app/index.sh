#!/bin/bash
set -euo pipefail

echo "Running MapReduce index pipeline"
input_file=${1:-/index/data/part-00000}  # HDFS path without hdfs:// prefix
echo "Using input: $input_file"

# Verify input exists
if ! hdfs dfs -test -e "$input_file"; then
    echo "Error: Input file $input_file does not exist in HDFS"
    hdfs dfs -ls "$(dirname "$input_file")"
    exit 1
fi

# Clean previous output
hdfs dfs -rm -r -f /tmp/index-output || true

# Run with proper option syntax
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
    -D mapreduce.job.name="Document_Indexer" \
    -files "/app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py" \
    -input "$input_file" \
    -output /tmp/index-output \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -inputformat org.apache.hadoop.mapred.TextInputFormat \
    -outputformat org.apache.hadoop.mapred.TextOutputFormat

echo "Indexing completed successfully"
hdfs dfs -ls /tmp/index-output