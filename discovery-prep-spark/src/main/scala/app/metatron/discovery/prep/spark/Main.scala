package app.metatron.discovery.prep.spark

import java.util
import java.util.HashMap

import scala.collection.JavaConverters._


object Main {

  var transformer: Transformer = null

  def main(args: Array[String]): Unit = {

    var argsInfo = args;

    if( args.length < 1) {
      argsInfo = this.getTestInfo();
    }

    SparkUtil.getSession(argsInfo(0));

    transformer = new Transformer(SparkUtil.getSession())
    transformer.process(argsInfo)

  }

  def javaCall(args: java.util.List[String]) = {
    if (transformer == null) {
      transformer = new Transformer(SparkUtil.getSession())
    }
    transformer.process(args.asScala.toArray)
  }

  ///////// TEST DATA //////////////////////////////////////////////////////////

  def getTestInfo(): Array[String] = {
    var argsList = new java.util.ArrayList[String]

    //////// prepPropertiesInfo
    val prepPropertiesInfo = new HashMap[String, Object]()
    prepPropertiesInfo.put("spark.master", "local")
    prepPropertiesInfo.put("spark.appname", "spark_prep")
    prepPropertiesInfo.put("spark.driver.host", "localhost")
    prepPropertiesInfo.put("hive.metastore.uris", "thrift://localhost:9083")

    argsList.add(Util.toJson(prepPropertiesInfo));

    /////////   datasetInfo
    val datasetInfo = new HashMap[String, Object]()
    datasetInfo.put("importType", "FILE")
    //datasetInfo.put("filePath", "file:///Users/nowone/Documents/GitHub/metatron-discovery-copy/discovery-prep-spark/src/test/resources/crime.csv") // put into HDFS before test

    datasetInfo.put("filePath", "hdfs://localhost:9000/tmp/test/crime.csv")
    datasetInfo.put("delimiter", ",")

    //datasetInfo.put("upstreamDatasetInfos", new util.ArrayList[_])
    datasetInfo.put("origTeddyDsId", "a74f9474-4633-425f-88ea-5a33d543c84c")


    val ruleStrings = new util.ArrayList[String]
    ruleStrings.add("header rownum: 1")

    ruleStrings.add("rename col: Population_ to: Population")
    ruleStrings.add("settype col: Population type: long ")
    ruleStrings.add("settype col: Date type: timestamp format: 'yyyy.MM.dd'")
    ruleStrings.add("settype col: Population  ")

    datasetInfo.put("ruleStrings", ruleStrings)

    argsList.add(Util.toJson(datasetInfo))

    ////////////////   snapshotInfo
    val snapshotInfo = new HashMap[String, Object]()
    //snapshotInfo.put("stagingBaseDir", "hdfs://localhost:9000/tmp/test");
    //snapshotInfo.put("ssType", "HDFS");
    snapshotInfo.put("ssType", "HIVE");

    //snapshotInfo.put("localBaseDir", "/Users/nowone/dev/temp");
    //snapshotInfo.put("ssType", "FILE");
    snapshotInfo.put("engine", "SPARK");
    //snapshotInfo.put("format", "CSV");
    snapshotInfo.put("compression", "NONE");
    //snapshotInfo.put("ssName", "crime_20180913_053230");
    snapshotInfo.put("ssName", "crime1");
    snapshotInfo.put("ssId", "6e3eec52-fc60-4309-b0de-a53f93e08ce9");
    //snapshotInfo.put("mode", "APPEND");

    argsList.add(Util.toJson(snapshotInfo))

    /////////   callbackInfo
    val callbackInfo = new HashMap[String, Object]()

    callbackInfo.put(PrepConst.OAUTH_TOKEN, "a74f9474-4633-425f-88ea-5a33d543c84c")

    argsList.add(Util.toJson(callbackInfo))


    //return
    argsList.asScala.toArray

  }

}
