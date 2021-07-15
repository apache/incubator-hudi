#!/bin/bash

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

#
# Usage: ./scripts/checkout_pr.sh
#
# Checkout a given branch. Assumes that the branch exists in remote.
# Run HoodieConfigDocGenerator class that generates markdown file with configurations.
#
set -eou pipefail

function printUsage {
  echo "Usage: $(basename "${0}") [-r REMOTE] [-f] <branch-name>" 2>&1
  echo '   -r  REMOTE remote to grab PR from (default: apache)'
  echo '   -f         force overwrite of local branch (default: fail if exists)'
  exit 1
}

if [[ ${#} -eq 0 ]]; then
  printUsage
fi

REMOTE="origin"
FORCE=""
while getopts ":r:f" arg; do
  case "${arg}" in
    r)
      REMOTE="${OPTARG}"
      ;;
    f)
      FORCE="--force"
      ;;
    ?)
      printUsage
      ;;
  esac
done
shift "$(($OPTIND -1))"

# Debug output
BRANCH_NAME=$1

# Checkout the branch.
#git fetch ${REMOTE} && git checkout ${BRANCH_NAME}
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Provide dependencies in the classpath
HUDI_UTIL_JAR=`ls -c $DIR/../packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_*SNAPSHOT.jar`
HUDI_SPARK_JAR=`ls -c $DIR/../packaging/hudi-spark-bundle/target/hudi-spark*.jar | grep -v sources | head -1`
# Run the class
java -cp $DIR/target/classes/:${HUDI_UTIL_JAR}:$HUDI_SPARK_JAR org.apache.hudi.utilities.HoodieConfigDocGenerator "$@"
