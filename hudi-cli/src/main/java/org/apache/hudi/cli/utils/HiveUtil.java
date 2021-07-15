/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cli.utils;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

/**
 * Hive connection related utilities.
 */
public class HiveUtil {

  private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  static {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + DRIVER_NAME + " in classpath. ", e);
    }
  }

  private static Connection getConnection(String jdbcUrl, String user, String pass) throws SQLException {
    return DriverManager.getConnection(jdbcUrl, user, pass);
  }

  public static long countRecords(String jdbcUrl, HoodieTableMetaClient source, String dbName, String user, String pass)
      throws SQLException {
    ResultSet rs = null;
    try (Connection conn = HiveUtil.getConnection(jdbcUrl, user, pass);
         Statement stmt = conn.createStatement()) {
      // stmt.execute("set mapred.job.queue.name=<queue_name>");
      stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
      stmt.execute("set hive.stats.autogather=false");
      rs = stmt.executeQuery(
          "select count(`_hoodie_commit_time`) as cnt from " + dbName + "." + source.getTableConfig().getTableName());
      long count = -1;
      if (rs.next()) {
        count = rs.getLong("cnt");
      }
      System.out.println("Total records in " + source.getTableConfig().getTableName() + " is " + count);
      return count;
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }

  public static long countRecords(String jdbcUrl, HoodieTableMetaClient source, String srcDb, int partitions,
      String user, String pass) throws SQLException {
    LocalDate date = LocalDate.now();
    date = date.minusDays(partitions);
    String endDateStr = date.getYear() + "-" + String.format("%02d", date.getMonthValue()) + "-"
        + String.format("%02d", date.getDayOfMonth());
    date = date.minusDays(partitions);
    String startDateStr = date.getYear() + "-" + String.format("%02d", date.getMonthValue()) + "-"
        + String.format("%02d", date.getDayOfMonth());
    System.out.println("Start date " + startDateStr + " and end date " + endDateStr);
    return countRecords(jdbcUrl, source, srcDb, startDateStr, endDateStr, user, pass);
  }

  private static long countRecords(String jdbcUrl, HoodieTableMetaClient source, String srcDb, String startDateStr,
      String endDateStr, String user, String pass) throws SQLException {
    ResultSet rs = null;
    try (Connection conn = HiveUtil.getConnection(jdbcUrl, user, pass);
         Statement stmt = conn.createStatement()) {
      // stmt.execute("set mapred.job.queue.name=<queue_name>");
      stmt.execute("set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
      stmt.execute("set hive.stats.autogather=false");
      rs = stmt.executeQuery(
          "select count(`_hoodie_commit_time`) as cnt from " + srcDb + "." + source.getTableConfig().getTableName()
              + " where datestr>'" + startDateStr + "' and datestr<='" + endDateStr + "'");
      if (rs.next()) {
        return rs.getLong("cnt");
      }
      return -1;
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }
}
