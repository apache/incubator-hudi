/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

import com.beust.jcommander.Parameter;
import com.uber.hoodie.DataSourceReadOptions;
import com.uber.hoodie.DataSourceWriteOptions;
import com.uber.hoodie.HoodieDataSourceHelpers;
import com.uber.hoodie.NonpartitionedKeyGenerator;
import com.uber.hoodie.SimpleKeyGenerator;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.config.AbstractCommandConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.hive.MultiPartKeysValueExtractor;
import com.uber.hoodie.hive.NonPartitionedExtractor;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Sample program that writes & reads hoodie datasets via the Spark datasource
 */
public class HoodieJavaApp {

  public static class Config extends AbstractCommandConfig {

    @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
    private String tablePath = "file:///tmp/hoodie/sample-table";

    @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
    private String tableName = "hoodie_test";

    @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
    private String tableType = HoodieTableType.COPY_ON_WRITE.name();

    @Parameter(names = {"--hive-sync", "-hv"}, description = "Enable syncing to hive")
    private Boolean enableHiveSync = false;

    @Parameter(names = {"--hive-db", "-hd"}, description = "hive database")
    private String hiveDB = "default";

    @Parameter(names = {"--hive-table", "-ht"}, description = "hive table")
    private String hiveTable = "hoodie_sample_test";

    @Parameter(names = {"--hive-user", "-hu"}, description = "hive username")
    private String hiveUser = "hive";

    @Parameter(names = {"--hive-password", "-hp"}, description = "hive password")
    private String hivePass = "hive";

    @Parameter(names = {"--hive-url", "-hl"}, description = "hive JDBC URL")
    private String hiveJdbcUrl = "jdbc:hive2://localhost:10000";

    @Parameter(names = {"--non-partitioned", "-np"}, description = "Use non-partitioned Table")
    private Boolean nonPartitionedTable = false;

    @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
    private Boolean useMultiPartitionKeys = false;
  }

  private static Logger logger = LogManager.getLogger(HoodieJavaApp.class);
  private final Config cfg;

  public HoodieJavaApp(Config cfg) {
    this.cfg = cfg;
  }

  public static void main(String[] args) throws Exception {
    Config cfg = new Config();
    cfg.parseCommandConfig(args);
    HoodieJavaApp cli = new HoodieJavaApp(cfg);
    cli.run();
  }

  public void run() throws Exception {

    // Spark session setup..
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP")
        .config("spark.serializer",
            "org.apache.spark.serializer.KryoSerializer").master("local[1]")
        .getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = null;
    if (cfg.nonPartitionedTable) {
      // All data goes to base-path
      dataGen = new HoodieTestDataGenerator(new String[]{""});
    } else {
      dataGen = new HoodieTestDataGenerator();
    }

    /**
     * Commit with only inserts
     */
    // Generate some input..
    List<String> records1 = DataSourceTestUtils.convertToStringList(
        dataGen.generateInserts("001"/* ignore */, 100));
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    // Save as hoodie dataset (copy on write)
    DataFrameWriter<Row> writer = inputDF1.write().format("com.uber.hoodie") // specify the hoodie source
        .option("hoodie.insert.shuffle.parallelism",
            "2") // any hoodie client config can be passed like this
        .option("hoodie.upsert.shuffle.parallelism",
            "2") // full list in HoodieWriteConfig & its package
        .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), cfg.tableType) // Hoodie Table Type
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(),
            DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL()) // insert
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(),
            "_row_key") // This is the record key
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(),
            "partition") // this is the partition to place it into
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(),
            "timestamp") // use to combine duplicate records in input/with disk val
        .option(HoodieWriteConfig.TABLE_NAME, cfg.tableName) // Used by hive sync and queries
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
            cfg.nonPartitionedTable ? NonpartitionedKeyGenerator.class.getCanonicalName() :
                SimpleKeyGenerator.class.getCanonicalName()) // Add Key Extractor
        .mode(
            SaveMode.Overwrite); // This will remove any existing data at path below, and create a

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(cfg.tablePath); // ultimately where the dataset will be placed
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, cfg.tablePath);
    logger.info("First commit at instant time :" + commitInstantTime1);

    /**
     * Commit that updates records
     */
    List<String> records2 = DataSourceTestUtils.convertToStringList(
        dataGen.generateUpdates("002"/* ignore */, 100));
    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));
    writer = inputDF2.write().format("com.uber.hoodie").option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), cfg.tableType) // Hoodie Table Type
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
            cfg.nonPartitionedTable ? NonpartitionedKeyGenerator.class.getCanonicalName() :
                SimpleKeyGenerator.class.getCanonicalName()) // Add Key Extractor
        .option(HoodieWriteConfig.TABLE_NAME, cfg.tableName).mode(SaveMode.Append);

    updateHiveSyncConfig(writer);
    writer.save(cfg.tablePath);
    String commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, cfg.tablePath);
    logger.info("Second commit at instant time :" + commitInstantTime1);

    /**
     * Read & do some queries
     */
    Dataset<Row> hoodieROViewDF = spark.read().format("com.uber.hoodie")
        // pass any path glob, can include hoodie & non-hoodie
        // datasets
        .load(cfg.tablePath + (cfg.nonPartitionedTable ? "/*" : "/*/*/*/*"));
    hoodieROViewDF.registerTempTable("hoodie_ro");
    spark.sql("describe hoodie_ro").show();
    // all trips whose fare was greater than 2.
    spark.sql("select fare, begin_lon, begin_lat, timestamp from hoodie_ro where fare > 2.0")
        .show();

    if (cfg.tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) {
      /**
       * Consume incrementally, only changes in commit 2 above. Currently only supported for COPY_ON_WRITE TABLE
       */
      Dataset<Row> hoodieIncViewDF = spark.read().format("com.uber.hoodie")
          .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(),
              DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),
              commitInstantTime1) // Only changes in write 2 above
          .load(
              cfg.tablePath); // For incremental view, pass in the root/base path of dataset

      logger.info("You will only see records from : " + commitInstantTime2);
      hoodieIncViewDF.groupBy(hoodieIncViewDF.col("_hoodie_commit_time")).count().show();
    }
  }

  /**
   * Setup configs for syncing to hive
   * @param writer
   * @return
   */
  private DataFrameWriter<Row> updateHiveSyncConfig(DataFrameWriter<Row> writer) {
    if (cfg.enableHiveSync) {
      logger.info("Enabling Hive sync to " + cfg.hiveJdbcUrl);
      writer = writer.option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), cfg.hiveTable)
          .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), cfg.hiveDB)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), cfg.hiveJdbcUrl)
          .option(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), cfg.hiveUser)
          .option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), cfg.hivePass)
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
      if (cfg.nonPartitionedTable) {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            NonPartitionedExtractor.class.getCanonicalName())
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "");
      } else if (cfg.useMultiPartitionKeys) {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "year,month,day")
            .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            MultiPartKeysValueExtractor.class.getCanonicalName());
      } else {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "dateStr");
      }
    }
    return writer;
  }
}
