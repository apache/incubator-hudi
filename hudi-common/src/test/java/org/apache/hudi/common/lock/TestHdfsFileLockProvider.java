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

package org.apache.hudi.common.lock;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestHdfsFileLockProvider {
  private static final Logger LOG = LogManager.getLogger(TestHdfsFileLockProvider.class);

  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static FileSystem fs;
  private static Configuration hadoopConf;
  private static String lockDir = "/tmp/lock";
  private static LockConfiguration lockConfiguration;

  @BeforeAll
  public static void setup() throws IOException {
    if (hdfsTestService == null) {
      hdfsTestService = new HdfsTestService();
      dfsCluster = hdfsTestService.start(true);
      hadoopConf = hdfsTestService.getHadoopConf();
      fs = FileSystem.get(hadoopConf);
    }

    Properties properties = new Properties();
    properties.setProperty(LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY, lockDir);
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    properties.setProperty(LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");

    lockConfiguration = new LockConfiguration(properties);
  }

  @AfterAll
  public static void tearDownClass() {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  @Test
  public void testAcquireLock() {
    HdfsFileLockProvider hdfsFileLockProvider = new HdfsFileLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(hdfsFileLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    hdfsFileLockProvider.unlock();
  }

  @Test
  public void testUnLock() {
    HdfsFileLockProvider hdfsFileLockProvider = new HdfsFileLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(hdfsFileLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    hdfsFileLockProvider.unlock();
    Assertions.assertTrue(hdfsFileLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReentrantLock() {
    HdfsFileLockProvider hdfsFileLockProvider = new HdfsFileLockProvider(lockConfiguration, hadoopConf);
    Assertions.assertTrue(hdfsFileLockProvider.tryLock(lockConfiguration.getConfig()
        .getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    try {
      Assertions.assertFalse(hdfsFileLockProvider.tryLock(lockConfiguration.getConfig()
          .getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY), TimeUnit.MILLISECONDS));
    } catch (HoodieLockException e) {
      // expected
    }
    hdfsFileLockProvider.unlock();
  }

  @Test
  public void testUnlockWithoutLock() {
    HdfsFileLockProvider hdfsFileLockProvider = new HdfsFileLockProvider(lockConfiguration, hadoopConf);
    hdfsFileLockProvider.unlock();
  }
}

