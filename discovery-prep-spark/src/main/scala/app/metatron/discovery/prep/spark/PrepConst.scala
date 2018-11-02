package app.metatron.discovery.prep.spark

/**
  * Created by nowone on 2018. 10. 12..
  */
object PrepConst {

  //SPARK
  val SPARK_APPNAME           = "spark.appname"
  val SPARK_MASTER            = "spark.master"

  // PrepProperties
  // app.metatron.discovery.domain.dataprep. PrepProperties
  val LOCAL_BASE_DIR          = "polaris.dataprep.localBaseDir"
  val STAGING_BASE_DIR        = "polaris.dataprep.stagingBaseDir"
  val HADOOP_CONF_DIR         = "polaris.dataprep.hadoopConfDir"
  val HIVE_HOSTNAME           = "polaris.dataprep.hive.hostname"
  val HIVE_PORT               = "polaris.dataprep.hive.port"
  val HIVE_USERNAME           = "polaris.dataprep.hive.username"
  val HIVE_PASSWORD           = "polaris.dataprep.hive.password"
  val HIVE_CUSTOM_URL         = "polaris.dataprep.hive.customUrl"
  val HIVE_METASTORE_URIS     = "polaris.dataprep.hive.metastoreUris"
  val SAMPLING_CORES          = "polaris.dataprep.sampling.cores"
  val SAMPLING_TIMEOUT        = "polaris.dataprep.sampling.timeout"
  val SAMPLING_LIMIT_ROWS     = "polaris.dataprep.sampling.limitRows"
  val SAMPLING_AUTO_TYPING    = "polaris.dataprep.sampling.autoTyping"
  val ETL_CORES               = "polaris.dataprep.etl.cores"
  val ETL_TIMEOUT             = "polaris.dataprep.etl.timeout"
  val ETL_LIMIT_ROWS          = "polaris.dataprep.etl.limitRows"
  val ETL_JAR                 = "polaris.dataprep.etl.jar"
  val ETL_JVM_OPTIONS         = "polaris.dataprep.etl.jvmOptions"

  // Constants dataSet Info
  val IMPORT_TYPE             = "importType"                // FILE, DB, HIVE
  val DELIMITER               = "delimiter"
  val FILE_PATH               = "filePath"
  val UPSTREAM_DATASET_INFOS  = "upstreamDatasetInfos"
  val RULE_STRINGS            = "ruleStrings"
  val ORIG_TEDDY_DS_ID        = "origTeddyDsId"
  val SOURCE_QUERY            = "sourceQuery"
  val CONNECT_URI             = "connectUri"
  val USERNAME                = "username"
  val PASSWORD                = "password"
  val IMPLEMENTOR             = "implementor" //ORACLE,MYSQL,POSTGRESQL,HIVE, PRESTO, TIBERO
  val HEADER                  = "header"
  val QUOTE                   = "quote"
  val QUOTE_ALL               = "quoteAll"
  val ESCAPE                  = "escape"
  val IGNORE_LEADING_WHITESPACE               = "ignoreLeadingWhiteSpace"
  val IGNORE_TRAILING_WHITESPACE               = "ignoreTrailingWhiteSpace"
  val MULTILINE               = "multiLine"



  val OAUTH_TOKEN             = "oauthToken"



  // snapshot info
  val SS_LOCAL_BASE_DIR       = "localBaseDir"     //${METATRON_HOME}/dataprep
  val SS_STAGING_BASE_DIR     = "stagingBaseDir"   //stagingBaseDir: hdfs://localhost:9000/user/hive/dataprep
  val SS_TYPE                 = "ssType"            // FILE,HDFS,JDBC,HIVE
  val SS_ENGINE               = "engine"
  val SS_FORMAT               = "format"
  val SS_COMPRESSION          = "compression"
  val SS_MODE                 = "mode"
  val SS_NAME                 = "ssName"
  val SS_ID                   = "ssId"

}
