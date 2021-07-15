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


# Simple examples of HoodieDeltaStreamer which read data from a mock HoodieExampleDataGenerator,
# this is an example for developers to define your own custom data source.

BASE_PATH=$(cd `dirname $0`; pwd)

${BASE_PATH}/hudi-delta-streamer \
--hoodie-conf hoodie.datasource.write.recordkey.field=uuid \
--hoodie-conf hoodie.datasource.write.partitionpath.field=driver \
--target-base-path /tmp/hoodie/deltastreamertable \
--table-type MERGE_ON_READ \
--target-table deltastreamertable \
--source-ordering-field ts \
--source-class org.apache.hudi.examples.common.RandomJsonSource \
--schemaprovider-class org.apache.hudi.examples.common.ExampleDataSchemaProvider \
--transformer-class org.apache.hudi.examples.common.IdentityTransformer \
--continuous
