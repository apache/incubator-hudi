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

package org.apache.hudi.utilities;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

public class HoodieCleaner {

  private static volatile Logger log = LogManager.getLogger(HoodieCleaner.class);

  /**
   * Config for Cleaner
   */
  private final Config cfg;

  /**
   * Filesystem used
   */
  private transient FileSystem fs;

  /**
   * Spark context
   */
  private transient JavaSparkContext jssc;

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private TypedProperties props;

  public HoodieCleaner(Config cfg, JavaSparkContext jssc) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.fs = FSUtils.getFs(cfg.basePath, jssc.hadoopConfiguration());
    this.props = cfg.propsFilePath == null ? UtilHelpers.buildProperties(cfg.configs)
        : UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating Cleaner with configs : " + props.toString());
  }

  public void run() throws Exception {
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig();
    HoodieWriteClient client = new HoodieWriteClient<>(jssc, hoodieCfg, false);
    client.clean();
  }

  private HoodieWriteConfig getHoodieClientConfig() throws Exception {
    return HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.basePath).withAutoCommit(false)
        .withProps(props).build();
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--target-base-path"}, description = "base path for the hoodie dataset to be cleaner.",
        required = true)
    public String basePath;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for cleaning")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    String dirName = new Path(cfg.basePath).getName();
    JavaSparkContext jssc = UtilHelpers.buildSparkContext("hoodie-cleaner-" + dirName, cfg.sparkMaster);
    new HoodieCleaner(cfg, jssc).run();
  }
}
