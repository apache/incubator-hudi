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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieLockException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A file based lock. This {@link LockProvider} implementation allows to lock table operations using HDFS
 * This is achieved by using the concept of acquiring the data out stream using Append option.
 */
public class HdfsFileLockProvider implements LockProvider<String> {
  private static final Logger LOG = LogManager.getLogger(HdfsFileLockProvider.class);

  private String lockDir;
  private FileSystem fs;
  protected LockConfiguration lockConfiguration;
  private DataOutputStream dataOutputStream;
  private String lockFile;

  public HdfsFileLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    this.lockConfiguration = lockConfiguration;
    this.lockDir = lockConfiguration.getConfig().getString(LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY);
    this.fs = FSUtils.getFs(lockDir, conf);
    this.lockFile = new Path(lockDir, "lockFile").toUri().toString();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) {
    try {
      int retryCount = lockConfiguration.getConfig().getInteger(LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY);
      int retryTimeout = lockConfiguration.getConfig().getInteger(LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY);
      for (int i = 0; i < retryCount; i++) {
        if (acquireLock()) {
          LOG.info(generateLogStatement(LockState.ACQUIRED, generateLogSuffixString()));
          return true;
        } else {
          Thread.sleep(retryTimeout);
        }
      }
    } catch (InterruptedException e) {
      throw new HoodieLockException("Failed to acquire file lock", e);
    }
    return false;
  }

  @Override
  public void unlock() {
    LOG.info(generateLogStatement(LockState.RELEASING, generateLogSuffixString()));
    if (dataOutputStream != null) {
      try {
        dataOutputStream.close();
        dataOutputStream = null;
      } catch (IOException e) {
        LOG.error(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()));
        throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE, generateLogSuffixString()), e);
      }
    }
    LOG.info(generateLogStatement(LockState.RELEASED, generateLogSuffixString()));
  }

  private boolean acquireLock() {
    if (lockDir == null || lockFile == null) {
      return false;
    }
    try {
      Path lockPath = new Path(lockDir);
      Path lockFilePath = new Path(lockFile);
      if (!fs.exists(lockPath)) {
        fs.mkdirs(lockPath);
      }
      if (!fs.exists(lockFilePath)) {
        // Pass the permissions during file creation itself
        fs.create(lockFilePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL),
            false, fs.getConf().getInt("io.file.buffer.size", 4096),
            fs.getDefaultReplication(lockFilePath), fs.getDefaultBlockSize(lockFilePath), null).close();
        // set permission forcefully
        fs.setPermission(lockFilePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      }
      dataOutputStream = fs.append(new Path(lockFile));
      return true;
    } catch (IOException e) {
      LOG.error("cannot acquire file lock: ", e);
      return false;
    }
  }

  private String generateLogSuffixString() {
    return StringUtils.join(" path ", lockDir, " and ", "lock file: lockFile");
  }

  private String generateLogStatement(LockState state, String suffix) {
    return StringUtils.join(state.name(), " lock at", suffix);
  }
}