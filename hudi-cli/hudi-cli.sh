#!/usr/bin/env bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOODIE_JAR=`ls $DIR/target/hudi-cli-*.jar | grep -v source | grep -v javadoc`

. "${DIR}"/conf/hudi-env.sh

if [ -z "$CLIENT_JAR" ]; then
  echo "Client jar location not set, please set it in conf/hudi-env.sh"
fi

OTHER_JARS=`ls ${DIR}/target/lib/* | grep -v 'hudi-[^/]*jar' | tr '\n' ':'`
HADOOP_JARS=${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/tools/lib/*

echo "Running : java -cp ${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:${HOODIE_JAR}:${OTHER_JARS}:${CLIENT_JAR}:${HADOOP_JARS} -DSPARK_CONF_DIR=${SPARK_CONF_DIR} -DHADOOP_CONF_DIR=${HADOOP_CONF_DIR} org.springframework.shell.Bootstrap $@"
java -cp ${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:${HOODIE_JAR}:${OTHER_JARS}:${CLIENT_JAR}:${HADOOP_JARS} -DSPARK_CONF_DIR=${SPARK_CONF_DIR} -DHADOOP_CONF_DIR=${HADOOP_CONF_DIR} org.springframework.shell.Bootstrap $@
