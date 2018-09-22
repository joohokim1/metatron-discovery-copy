package app.metatron.discovery.prep.spark

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser
import app.metatron.discovery.prep.spark.rule.{PrepHeader, PrepRename, PrepReplace}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty

class Transformer(@BeanProperty spark: SparkSession) {
  // Constants
  val IMPORT_TYPE            = "importType"           : String
  val DELIMITER              = "delimiter"            : String
  val FILE_PATH              = "filePath"             : String
  val UPSTREAM_DATASET_INFOS = "upstreamDatasetInfos" : String
  val RULE_STRINGS           = "ruleStrings"          : String
  val ORIG_TEDDY_DS_ID       = "origTeddyDsId"        : String

  // Static variables
  val RuleVisitorParser = new RuleVisitorParser

  // Main entrance of this program
  def process(args: Array[String]) = {

    val jsonPrepProperties = args(0)
    val jsonDatasetInfo    = args(1)
    val jsonSnapshotInfo   = args(2)
    val jsonCallbackInfo   = args(3)

    println(f"jsonPrepProperties=$jsonPrepProperties")
    println(f"jsonDatasetInfo   =$jsonDatasetInfo")
    println(f"jsonSnapshotInfo  =$jsonSnapshotInfo")
    println(f"jsonCallbackInfo  =$jsonCallbackInfo")

    val mapPrepProperties = Util.toMap[Object](jsonPrepProperties)
    val mapDatasetInfo    = Util.toMap[Object](jsonDatasetInfo)
    val mapSnapshotInfo   = Util.toMap[Object](jsonSnapshotInfo)
    val mapCallbackInfo   = Util.toMap[Object](jsonCallbackInfo)

    SparkUtil.registerUdfs()

    var df = load(mapDatasetInfo)
    df.show()

    df = transform(df, mapDatasetInfo)
    df.show()
  }

  def load(mapDatasetInfo: Map[String, Object]): DataFrame = {
    var df: DataFrame = null
    println("load() start")

    for (key <- List(IMPORT_TYPE, DELIMITER, FILE_PATH, UPSTREAM_DATASET_INFOS, RULE_STRINGS, ORIG_TEDDY_DS_ID)) {
      println(f"$key=${mapDatasetInfo(key)}")
    }

    Util.getMapListFromMap(mapDatasetInfo, UPSTREAM_DATASET_INFOS).foreach(load)

    mapDatasetInfo.get(IMPORT_TYPE).get.asInstanceOf[String] match {
      case "FILE" => return loadFile(mapDatasetInfo)
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
    var newDf: DataFrame = df
    val ruleStrings = Util.getStringListFromMap(mapDatasetInfo, RULE_STRINGS)

    println("transform() start")

    ruleStrings.foreach(ruleString => newDf = applyRule(newDf, ruleString))

    println("transform() done")
    newDf
  }

  def applyRule(df: DataFrame, ruleString: String): DataFrame = {
    println(f"applyRule: $ruleString")

    val rule = RuleVisitorParser.parse(ruleString)

    rule.getName match {
      case "rename" => return PrepRename(rule).transform(df)
      case "header" => return PrepHeader(rule).transform(df)
      case "replace" => return PrepReplace(rule).transform(df)
    }
  }

}
