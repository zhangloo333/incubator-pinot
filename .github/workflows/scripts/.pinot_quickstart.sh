#!/bin/bash -x
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Java version
java -version

# ThirdEye related changes
COMMIT_BEFORE=$(jq -r ".pull_request.base.sha" "${GITHUB_EVENT_PATH}")
COMMIT_AFTER=$(jq -r ".pull_request.head.sha" "${GITHUB_EVENT_PATH}")
echo "${COMMIT_BEFORE}"
echo "${COMMIT_AFTER}"
git log "${COMMIT_BEFORE}"
git log "${COMMIT_AFTER}"
git fetch
git diff --name-only "${COMMIT_BEFORE}...${COMMIT_AFTER}"
git diff --name-only "${COMMIT_BEFORE}...${COMMIT_AFTER}" | egrep '^(thirdeye)'
if [ $? -eq 0 ]; then
  echo 'Skip ThirdEye tests for Quickstart'
  exit 0
fi

# Build
mvn clean package -DskipTests -Pbin-dist

# Quickstart
DIST_BIN_DIR=`ls -d pinot-distribution/target/apache-pinot-*/apache-pinot-*`
cd ${DIST_BIN_DIR}

# Test quick-start-batch
bin/quick-start-batch.sh &
PID=$!

PASS=0
sleep 30
for i in $(seq 1 200)
do
  COUNT_STAR_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from baseballStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
  if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]]; then
    if [ "${COUNT_STAR_RES}" -eq 97889 ]; then
      PASS=1
      break
    fi
  fi
  sleep 1
done

if [ "${PASS}" -eq 0 ]; then
  echo 'Batch Quickstart failed: Cannot get correct result for count star query.'
  exit 1
fi

kill -9 $PID
rm -rf /tmp/PinotAdmin/zkData

# Test quick-start-streaming
bin/quick-start-streaming.sh &
PID=$!

PASS=0
RES_1=0
sleep 30

for i in $(seq 1 200)
do
  COUNT_STAR_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from meetupRsvp limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
 if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]]; then
    if [ "${COUNT_STAR_RES}" -gt 0 ]; then
      if [ "${RES_1}" -eq 0 ]; then
        RES_1=${COUNT_STAR_RES}
        continue
      fi
    fi
    if [ "${COUNT_STAR_RES}" -gt "${RES_1}" ]; then
      PASS=1
      break
    fi
  fi
  sleep 1
done

if [ "${PASS}" -eq 0 ]; then
  if [ "${RES_1}" -eq 0 ]; then
    echo 'Streaming Quickstart test failed: Cannot get correct result for count star query.'
    exit 1
  fi
  echo 'Streaming Quickstart test failed: Cannot get incremental counts for count star query.'
  exit 1
fi

kill -9 $PID
rm -rf /tmp/PinotAdmin/zkData

# Test quick-start-hybrid
cd bin
./quick-start-hybrid.sh &
PID=$!

PASS=0
RES_1=0
sleep 30
for i in $(seq 1 200)
do
  COUNT_STAR_RES=`curl -X POST --header 'Accept: application/json'  -d '{"sql":"select count(*) from airlineStats limit 1","trace":false}' http://localhost:8000/query/sql | jq '.resultTable.rows[0][0]'`
  if [[ "${COUNT_STAR_RES}" =~ ^[0-9]+$ ]]; then
    if [ "${COUNT_STAR_RES}" -gt 0 ]; then
      if [ "${RES_1}" -eq 0 ]; then
        RES_1=${COUNT_STAR_RES}
        continue
      fi
    fi
    if [ "${COUNT_STAR_RES}" -gt "${RES_1}" ]; then
      PASS=1
      break
    fi
  fi
  sleep 1
done

if [ "${PASS}" -eq 0 ]; then
  if [ "${RES_1}" -eq 0 ]; then
    echo 'Hybrid Quickstart test failed: Cannot get correct result for count star query.'
    exit 1
  fi
  echo 'Hybrid Quickstart test failed: Cannot get incremental counts for count star query.'
  exit 1
fi

kill -9 $PID
rm -rf /tmp/PinotAdmin/zkData

cd ../../../../../
pwd
mvn clean > /dev/null

exit 0
