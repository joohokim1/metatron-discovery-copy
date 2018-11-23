package app.metatron.discovery.prep.spark

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser
import app.metatron.discovery.prep.spark.rule._
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

class Transformer(spark: SparkSession) {
  val logger = LogManager.getLogger(this.getClass.getName)

  // Static variables
  val RuleVisitorParser = new RuleVisitorParser


  // Main entrance of this program
  def process(args: Array[String]) = {

    val jsonPrepProperties = args(0)
    val jsonDatasetInfo    = args(1)
    val jsonSnapshotInfo   = args(2)
    val jsonCallbackInfo   = args(3)

    this.logger.info(f"jsonPrepProperties=$jsonPrepProperties")
    this.logger.info(f"jsonDatasetInfo   =$jsonDatasetInfo")
    this.logger.info(f"jsonSnapshotInfo  =$jsonSnapshotInfo")
    this.logger.info(f"jsonCallbackInfo  =$jsonCallbackInfo")

    val mapPrepProperties = Util.toMap[Object](jsonPrepProperties)
    val mapDatasetInfo    = Util.toMap[Object](jsonDatasetInfo)
    val mapSnapshotInfo   = Util.toMap[Object](jsonSnapshotInfo)
    val mapCallbackInfo   = Util.toMap[Object](jsonCallbackInfo)

    Try {

      SparkUtil.registerUdfs()

      var df = SparkUtil.load(mapDatasetInfo)
      df.show()

      df = transform(df, mapDatasetInfo)
      df.show(false)


      SparkUtil.save(df, mapSnapshotInfo)

    } match {
      case Success(result) => this.callbackSuccess(mapCallbackInfo, Map( "result" -> "success"))
      case Failure(exception) =>  this.callbackError(exception, mapCallbackInfo, Map("result" -> "error") )

    }

  }


  def load(mapDatasetInfo: Map[String, Object]): DataFrame = {
    var df: DataFrame = null

    this.logger.info("load() start")

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

    this.logger.info("load() done")

    df
  }

  def loadFile(mapDatasetInfo: Map[String, Object]): DataFrame = {
    val filePath: String = Util.getStringFromMap(mapDatasetInfo, PrepConst.FILE_PATH)

    spark.read.format("csv").option("header", "false").load(filePath)
  }

  def transform(df: DataFrame, mapDatasetInfo: Map[String, Object]): DataFrame = {
    var newDf: DataFrame = df
    val ruleStrings = Util.getStringListFromMap(mapDatasetInfo, PrepConst.RULE_STRINGS)

    this.logger.info("transform() start")

    ruleStrings.foreach(ruleString => newDf = applyRule(newDf, ruleString))

    this.logger.info("transform() done")
    newDf
  }

  def applyRule(df: DataFrame, ruleString: String): DataFrame = {
    this.logger.debug(f"applyRule: $ruleString")

    val rule = RuleVisitorParser.parse(ruleString)

    rule.getName match {
      case "rename"         => return PrepRename(rule).transform(df)
      case "header"         => return PrepHeader(rule).transform(df)
      case "drop"           => return PrepDrop(rule).transform(df)
      case "replace"        => return PrepReplace(rule, ruleString).transform(df)
      case "settype"        => return PrepSetType(rule).transform(df)
      case "keep"           => return PrepKeep(rule).transform(df)
      case "delete"         => return PrepDelete(rule).transform(df)
      case "split"          => return PrepSplit(rule).transform(df)
      case "merge"          => return PrepMerge(rule).transform(df)
      case "nest"           => return PrepNest(rule).transform(df)
      case "unnest"         => return PrepUnnest(rule).transform(df)
    }
  }

  /**
    * call key 정의 필요
    * @param mapCallbackInfo
    * @param dataMap
    */
  def callbackSuccess( mapCallbackInfo: Map[String, Object], dataMap: Map[String, String] = null  ): Unit = {

    val scheme = "http"
    val host = "localhost"
    val port = "8018" //Util.getStringFromMap(mapCallbackInfo, "port")
    val path = "/api/preparationsnapshots/"
    val ssId = ""

    val updateUrl = scheme +"://" + host +":" + port + path + ssId;


    logger.info("Callback URL:" + updateUrl);

    val oauthToken = Util.getStringFromMap(mapCallbackInfo, PrepConst.OAUTH_TOKEN);

    val headerMap = Map(
      "Content-Type" -> "application/json;charset=UTF-8",
      "Accept" -> "application/json, text/plain, */*",
      "Authorization" -> oauthToken
    )

    // HttpUtil.patch(updateUrl, dataMap, headerMap)

  }


  /**
    * call key 정의 필요
    * @param exception
    * @param mapCallbackInfo
    * @param dataMap
    */
  def callbackError(exception: Throwable, mapCallbackInfo: Map[String, Object], dataMap: Map[String, String] = null  ): Unit = {

    this.logger.error(exception);

    val scheme = "http"
    val host = "localhost"
    val port = "8018" //Util.getStringFromMap(mapCallbackInfo, "port")
    val path = "/api/preparationsnapshots/"
    val ssId = ""

    val updateUrl = scheme +"://" + host +":" + port + path + ssId;


    logger.info("Callback URL:" + updateUrl);

    val oauthToken = Util.getStringFromMap(mapCallbackInfo, PrepConst.OAUTH_TOKEN);

    val headerMap = Map(
      "Content-Type" -> "application/json;charset=UTF-8",
      "Accept" -> "application/json, text/plain, */*",
      "Authorization" -> oauthToken
    )

    // HttpUtil.patch(updateUrl, dataMap, headerMap)

  }

}
