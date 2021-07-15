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

class TestMergeIntoTable2 extends TestHoodieSqlBase {

  test("Test MergeInto for MOR table 2") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // Create a mor partitioned table.
      spark.sql(
        s"""
           | create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long,
           |  dt string
           | ) using hudi
           | options (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
           | partitioned by(dt)
           | location '${tmp.getCanonicalPath}'
         """.stripMargin)
      // Insert data which matched insert-condition.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )

      // Insert data which not matched insert-condition.
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 2 as id, 'a2' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when not matched and s0.id % 2 = 1 then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )

      // Update data which not matched update-condition
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 0 then update set *
           | when matched and s0.id % 3 = 2 then delete
           | when not matched then insert *
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 10, "2021-03-21")
      )

      // Update data which matched update-condition
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 1 then update set id = s0.id, name = s0.name,
           |  price = s0.price * 2, ts = s0.ts, dt = s0.dt
           | when not matched then insert (id,name,price,ts,dt) values(s0.id, s0.name, s0.price, s0.ts, s0.dt)
         """.stripMargin
      )
      checkAnswer(s"select id,name,price,dt from $tableName")(
        Seq(1, "a1", 22, "2021-03-21")
      )

      // Delete data which matched update-condition
      spark.sql(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 11 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 0 then update set id = s0.id, name = s0.name,
           |  price = s0.price * 2, ts = s0.ts, dt = s0.dt
           | when matched and s0.id % 2 = 1 then delete
           | when not matched then insert (id,name,price,ts,dt) values(s0.id, s0.name, s0.price, s0.ts, s0.dt)
         """.stripMargin
      )
      checkAnswer(s"select count(1) from $tableName")(
        Seq(0)
      )

      checkException(
        s"""
           | merge into $tableName as t0
           | using (
           |  select 1 as id, 'a1' as name, 10 as price, 1000 as ts, '2021-03-21' as dt
           | ) as s0
           | on t0.id = s0.id
           | when matched and s0.id % 2 = 1 then update set id = s0.id, name = s0.name,
           |  price = s0.price + t0.price, ts = s0.ts, dt = s0.dt
         """.stripMargin
      )("assertion failed: Target table's field(price) cannot be the right-value of the update clause for MOR table.")
    }
  }
}
