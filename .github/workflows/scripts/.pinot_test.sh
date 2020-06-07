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


jdk_version() {
  local result
  local java_cmd='java'
  local IFS=$'\n'
  # remove \r for Cygwin
  local lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
  if [[ -z $java_cmd ]]
  then
    result=no_java
  else
    for line in $lines; do
      if [[ (-z $result) && ($line = *"version \""*) ]]
      then
        local ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
        # on macOS, sed doesn't support '?'
        if [[ $ver = "1."* ]]
        then
          result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
        else
          result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
        fi
      fi
    done
  fi
  echo "$result"
}

# ThirdEye related changes
JAVA_VER="$(jdk_version)"
echo "${JAVA_VER}"
printenv
cat "${GITHUB_EVENT_PATH}"
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
  echo 'ThirdEye changes.'

  if [ "$JAVA_VER" != 8 ]; then
    echo 'Skip ThirdEye tests for version other than jdk8.'
    exit 0
  fi

  if [ "${RUN_INTEGRATION_TESTS}" == false ]; then
    echo 'Skip ThirdEye tests when integration tests off'
    exit 0
  fi

  cd thirdeye
  mvn test
  failed=$?
  if [ $failed -eq 0 ]; then
    exit 0
  else
    exit 1
  fi
fi

# Only run tests for JDK 8
if [ "$JAVA_VER" != 8 ]; then
  echo 'Skip tests for version other than jdk8.'
  exit 0
fi

passed=0

# Only run integration tests if needed
if [ "$RUN_INTEGRATION_TESTS" != false ]; then
  mvn test -B -P travis,travis-integration-tests-only
  if [ $? -eq 0 ]; then
    passed=1
  fi
else
  mvn test -B -P travis,travis-no-integration-tests
  if [ $? -eq 0 ]; then
    passed=1
  fi
fi

if [ $passed -eq 1 ]; then
  # Only send code coverage data if passed
  bash <(cat .codecov_bash)
  exit 0
else
  exit 1
fi
