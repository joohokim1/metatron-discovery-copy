package app.metatron.discovery.prep.spark

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser
import app.metatron.discovery.prep.spark.rule._
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class Transformer(spark: SparkSession) {


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

    Try {

      SparkUtil.registerUdfs()

      var df = SparkUtil.load(mapDatasetInfo)
      df.show()

      df = transform(df, mapDatasetInfo)
      df.show()


      SparkUtil.save(df, mapSnapshotInfo)

    } match {
      case Success(result) => this.callback(mapCallbackInfo, Map( "result" -> "success"))
      case Failure(exception) => print(exception); this.callback(mapCallbackInfo, Map( "result" -> "error"))
    }

  }


  def load(mapDatasetInfo: Map[String, Object]): DataFrame = {
    var df: DataFrame = null
    println("load() start")

    /*
    for (key <- List(IMPORT_TYPE, DELIMITER, FILE_PATH, UPSTREAM_DATASET_INFOS, RULE_STRINGS, ORIG_TEDDY_DS_ID)) {
      println(f"$key=${mapDatasetInfo(key)}")
    }

    Util.getMapListFromMap(mapDatasetInfo, UPSTREAM_DATASET_INFOS).foreach(load)
    */

    mapDatasetInfo.get(PrepConst.IMPORT_TYPE).get.asInstanceOf[String] match {
      case "FILE" => return loadFile(mapDatasetInfo)
      case _ => assert(false)
    }

    println("load() done")
    df
  }

  def loadFile(mapDatasetInfo: Map[String, Object]): DataFrame = {
    val filePath: String = Util.getStringFromMap(mapDatasetInfo, PrepConst.FILE_PATH)

    spark.read.format("csv").option("header", "false").load(filePath)
  }

  def transform(df: DataFrame, mapDatasetInfo: Map[String, Object]): DataFrame = {
    var newDf: DataFrame = df
    val ruleStrings = Util.getStringListFromMap(mapDatasetInfo, PrepConst.RULE_STRINGS)

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
      case "settype" => return PrepSetType(rule).transform(df)
      case "keep" => return PrepKeep(rule).transform(df)
    }
  }

  /**
    * call key 정의 필요
    * @param mapCallbackInfo
    * @param dataMap
    */
  def callback( mapCallbackInfo: Map[String, Object], dataMap: Map[String, String] = null  ): Unit = {

    val scheme = "http"
    val host = "localhost"
    val port = "8018" //Util.getStringFromMap(mapCallbackInfo, "port")
    val path = "/api/preparationsnapshots/"
    val ssId = ""

    val updateUrl = scheme +"://" + host +":" + port + path + ssId;

    val log = LogManager.getRootLogger
    log.info("Callback URL:" + updateUrl);

    val oauthToken = Util.getStringFromMap(mapCallbackInfo, PrepConst.OAUTH_TOKEN);

    val headerMap = Map(
      "Content-Type" -> "application/json;charset=UTF-8",
      "Accept" -> "application/json, text/plain, */*",
      "Authorization" -> oauthToken
    )

    // HttpUtil.patch(updateUrl, dataMap, headerMap)

  }

}
