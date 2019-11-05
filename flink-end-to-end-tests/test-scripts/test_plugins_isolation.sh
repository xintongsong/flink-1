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

dummy_fs_setup

OUTPUT_LOCATION="${TEST_DATA_DIR}/out/wc_out-$RANDOM"

mkdir -p "${TEST_DATA_DIR}"

start_cluster

# Class isolation will be checked in the program jar.
TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-plugins-test/wordcount/target/flink-plugins-test-WordCount.jar
PROGRAM_ARGS="--input1 dummy://localhost/words --input2 anotherDummy://localhost/words --output $OUTPUT_LOCATION"
$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR $PROGRAM_ARGS

OUTPUT=`cat $OUTPUT_LOCATION`

# Two same inputs, so the count of words is double.
EXPECTED_RESULTS=("hello,2" "world,4" "how,2" "are,2" "you,2" "my,2" "dear,4")

for expected_result in ${EXPECTED_RESULTS[@]}; do
    if [[ ! "$OUTPUT" =~ $expected_result ]]; then
        echo "Output does not contain '$expected_result' as required"
        exit 1
    fi
done
