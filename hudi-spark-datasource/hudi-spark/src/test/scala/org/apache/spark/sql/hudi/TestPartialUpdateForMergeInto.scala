/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

class TestPartialUpdateForMergeInto extends TestHoodieSqlBase {

  test("Test Partial Update") {
    withTempDir { tmp =>
      // TODO after we support partial update for MOR, we can add test case for 'mor'.
      Seq("cow").foreach { tableType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             | id int,
             | name string,
             | price double,
             | _ts long
             |) using hudi
             |options(
             | type ='$tableType',
             | primaryKey = 'id',
             | preCombineField = '_ts'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
          """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")

        spark.sql(
          s"""
             |merge into $tableName t0
             |using ( select 1 as id, 'a1' as name, 12 as price, 1001 as ts) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price, _ts = s0.ts
             |""".stripMargin)
        checkAnswer(s"select id, name, price, _ts from $tableName")(
          Seq(1, "a1", 12.0, 1001)
        )

        val tableName2 = generateTableName
        spark.sql(
          s"""
             |create table $tableName2 (
             | id int,
             | name string,
             | price double
             |) using hudi
             |options(
             | type ='$tableType',
             | primaryKey = 'id'
             |)
             |location '${tmp.getCanonicalPath}/$tableName2'
          """.stripMargin)
        spark.sql(s"insert into $tableName2 values(1, 'a1', 10)")

        spark.sql(
          s"""
             |merge into $tableName2 t0
             |using ( select 1 as id, 'a1' as name, 12 as price) s0
             |on t0.id = s0.id
             |when matched then update set price = s0.price
             |""".stripMargin)
        checkAnswer(s"select id, name, price from $tableName2")(
          Seq(1, "a1", 12.0)
        )
      }
    }
  }

  test("Test MergeInto Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |options(
         | type = 'cow',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)

    checkException(
      s"""
         |merge into $tableName t0
         |using ( select 1 as id, 'a1' as name, 12 as price) s0
         |on t0.id = s0.id
         |when matched then update set price = s0.price
      """.stripMargin)(
      "Missing specify value for the preCombineField: _ts in merge-into update action. " +
        "You should add '... update set _ts = xx....' to the when-matched clause.;")

    val tableName2 = generateTableName
    spark.sql(
      s"""
         |create table $tableName2 (
         | id int,
         | name string,
         | price double,
         | _ts long
         |) using hudi
         |options(
         | type = 'mor',
         | primaryKey = 'id',
         | preCombineField = '_ts'
         |)""".stripMargin)

    checkException(
      s"""
         |merge into $tableName2 t0
         |using ( select 1 as id, 'a1' as name, 12 as price) s0
         |on t0.id = s0.id
         |when matched then update set price = s0.price
        """.stripMargin)(
      "Missing specify the value for target field: 'id' in merge into update action for MOR table. " +
        "Currently we cannot support partial update for MOR, please complete all the target fields " +
        "just like '...update set id = s0.id, name = s0.name ....';")
  }
}
