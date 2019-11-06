#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_dummy_fs.sh
source "$(dirname "$0")"/common_yarn_docker.sh

dummy_fs_setup

mkdir -p "${TEST_DATA_DIR}"

# Class isolation will be checked in the program jar.
TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-plugins-test/wordcount/target/flink-plugins-test-WordCount.jar
TARGET_PROGRAM_JAR="/tmp/flink-plugins-test-WordCount.jar"
INPUT_ARGS="--input1 dummy://localhost/words --input2 anotherDummy://localhost/words"
OUTPUT_PATH="hdfs:///user/hadoop-user/wc-out-$RANDOM"

docker cp $TEST_PROGRAM_JAR master:$TARGET_PROGRAM_JAR

if docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
   /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -ys 1 -ytm 1000 -yjm 1000 \
   -p 1 $TARGET_PROGRAM_JAR $INPUT_ARGS --output $OUTPUT_PATH";
then
    OUTPUT=$(get_output $OUTPUT_PATH)
    echo "$OUTPUT"
else
    echo "Running the job failed."
    copy_and_show_logs
    exit 1
fi

# Two same inputs, so the count of words is double.
EXPECTED_RESULTS=("hello,2" "world,4" "how,2" "are,2" "you,2" "my,2" "dear,4")

for expected_result in ${EXPECTED_RESULTS[@]}; do
    if [[ ! "$OUTPUT" =~ $expected_result ]]; then
        echo "Output does not contain '$expected_result' as required"
        exit 1
    fi
done