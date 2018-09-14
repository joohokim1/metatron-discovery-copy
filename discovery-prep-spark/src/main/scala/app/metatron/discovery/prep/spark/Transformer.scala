package app.metatron.discovery.prep.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
class Transformer(spark: SparkSession) {

  val IMPORT_TYPE = "importType" : String
  val DELIMITER = "delimiter" : String
  val FILE_PATH = "filePath" : String
  val UPSTREAM_DATASET_INFOS = "upstreamDatasetInfos" : String
  val RULE_STRINGS = "ruleStrings" : String
  val ORIG_TEDDY_DS_ID = "origTeddyDsId" : String

  def process(args: Array[String], spark: SparkSession = null): Unit = {
    if (spark == null) {
      val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
      println("Spark session created!")
    }

    val jsonPrepProperties = args.apply(0)
    val jsonDatasetInfo    = args.apply(1)
    val jsonSnapshotInfo   = args.apply(2)
    val jsonCallbackInfo   = args.apply(3)

    println(f"jsonPrepProperties=$jsonPrepProperties")
    println(f"jsonDatasetInfo=$jsonDatasetInfo")
    println(f"jsonSnapshotInfo=$jsonSnapshotInfo")
    println(f"jsonCallbackInfo=$jsonCallbackInfo")

    val mapDatasetInfo = Util.toMap[Object](jsonDatasetInfo)

    var df = load(mapDatasetInfo)
    df.show()

    df = transform(df, mapDatasetInfo)
    df.show()
  }

  def load(mapDatasetInfo: Map[String, Object]): DataFrame = {
    println("load() start")

    for (key <- List(IMPORT_TYPE, DELIMITER, FILE_PATH, UPSTREAM_DATASET_INFOS, RULE_STRINGS, ORIG_TEDDY_DS_ID)) {
      println(f"$key=${mapDatasetInfo(key)}")
    }

    mapDatasetInfo(UPSTREAM_DATASET_INFOS).asInstanceOf[List[Map[String, Object]]].foreach(load)

    var df: DataFrame = null

    mapDatasetInfo.get(IMPORT_TYPE).get.asInstanceOf[String] match {
      case "FILE" => df = loadFile(mapDatasetInfo)
      case _ => assert(false)
    }

    println("load() done")
    df
  }

  def loadFile(mapDatasetInfo: Map[String, Object]): DataFrame = {
    val filePath: String = Util.getStringFromMap(mapDatasetInfo, FILE_PATH)

    spark.read.format("csv").option("header", "false").load(filePath)
  }

  def transform(df: DataFrame, mapDatasetInfo: Map[String, Object]): DataFrame = {
    println("transform() start")

    val ruleStrings = Util.getStringListFromMap(mapDatasetInfo, RULE_STRINGS)
    ruleStrings.foreach(ruleString => applyRule(df, ruleString))

    println("transform() done")
    df
  }

  def applyRule(df: DataFrame, ruleString: String): DataFrame = {
    println(f"applyRule: $ruleString")
    df
  }

//  def printElem(map: Map[String, Object], key: String) = {
//    println(f"$key=${map(key)}")
//  }
//
//  def printElem(map: Any, tuple: (String, Object)) = {
//  }

}
