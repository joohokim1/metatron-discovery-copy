package app.metatron.discovery.prep.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

object SparkUtil {
  //val spark = Main.getSparkSession()

  private var spark: SparkSession = null;


  def getContext(): SparkContext = {

    if( this.spark == null ){
      this.getSession()
    }


    this.spark.sparkContext;
  }

  def getSession(): SparkSession = {
    if( this.spark == null){
      val conf = Map("spark.master" -> "local", "spark.appname" -> "spark_prep", "spark.driver.host" -> "localhost", "hive.metastore.uris" -> "thrift://localhost:9083")
      this.getSession(conf)
    }

    this.spark
  }

  def getSession(jsonString: String): SparkSession = {
    this.getSession(Util.toMap[Object](jsonString))
  }

  def getSession( conf:Map[String, Object]): SparkSession = {

    if( this.spark == null) {
      var newConf = conf;

      var sparkBuilder = SparkSession.builder();

      if( newConf.get(PrepConst.SPARK_APPNAME).isEmpty){
        newConf = newConf + (PrepConst.SPARK_APPNAME -> "spark_prep".asInstanceOf[Object])
      }

      if( newConf.get(PrepConst.SPARK_MASTER).isEmpty){
        newConf = newConf + (PrepConst.SPARK_MASTER -> "local".asInstanceOf[Object])
      }

      if ( newConf.get(PrepConst.HIVE_METASTORE_URIS).isEmpty && conf.get(PrepConst.HIVE_HOSTNAME).isDefined  ) {
        val value = Util.getStringFromMap(newConf, PrepConst.HIVE_HOSTNAME);

        newConf = newConf + ("hive.metastore.uris" -> ("thrift://"+value+":9083").asInstanceOf[Object])
      }

      if( newConf.get("hive.metastore.uris").isEmpty){
        newConf = newConf + ("hive.metastore.uris" -> ("thrift://localhost:9083").asInstanceOf[Object])
      }

      conf.keys.foreach( (key) => {
          val value = Util.getStringFromMap(newConf, key);
          if (PrepConst.SPARK_MASTER == key) {
            sparkBuilder = sparkBuilder.master(value)
          } else if (PrepConst.SPARK_APPNAME == key) {
            sparkBuilder = sparkBuilder.appName(value)
          } else {
            sparkBuilder = sparkBuilder.config(key, value)
          }

        }
      )

      this.spark = sparkBuilder
        .enableHiveSupport()
        .getOrCreate()


    }

    this.spark

  }

  def getDbDriver(implementor: String) = implementor match {
    case "ORACLE" => "oracle.jdbc.OracleDriver"
    case "MYSQL" => "com.mysql.jdbc.Driver"
    case "POSTGRESQL" => "org.postgresql.Driver"
    case "HIVE" => "org.apache.hive.jdbc.HiveDriver"
    case "PRESTO" => "com.facebook.presto.jdbc.PrestoDriver"
    case "TIBERO" => "com.tmax.tibero.jdbc.TbDriver"
  }

  /**
    * 파일 정보 필요
    * @param mapDatasetInfo
    * @return
    */
  def load( mapDatasetInfo: Map[String, Object]): DataFrame = {

    val importType = Util.getStringFromMap(mapDatasetInfo, PrepConst.IMPORT_TYPE)

    val df = importType match {
      case "FILE" => {

        val filePath: String = Util.getStringFromMap(mapDatasetInfo, PrepConst.FILE_PATH)

        val options = if(  mapDatasetInfo.get(PrepConst.DELIMITER).isDefined) {
          val delimiter = Util.getStringFromMap(mapDatasetInfo, PrepConst.DELIMITER)
          Map("header" -> "false", "delimiter" -> delimiter)
        } else {
          Map("header" -> "false")
        }

        this.loadFile("csv", filePath, options)
      }
      case "DB" => {

         //ORACLE,MYSQL,POSTGRESQL,HIVE, PRESTO, TIBERO

        val implementor = Util.getStringFromMap(mapDatasetInfo, PrepConst.IMPLEMENTOR)
        val sourceQuery = Util.getStringFromMap(mapDatasetInfo, PrepConst.SOURCE_QUERY)
        val connectUri  = Util.getStringFromMap(mapDatasetInfo, PrepConst.CONNECT_URI)
        val username    = Util.getStringFromMap(mapDatasetInfo, PrepConst.USERNAME)
        val password    = Util.getStringFromMap(mapDatasetInfo, PrepConst.PASSWORD)

        val connectionProperties = new java.util.Properties()
        connectionProperties.put("driver", this.getDbDriver(implementor))
        connectionProperties.put("user", username)
        connectionProperties.put("password", password)
        //connectionProperties.put("dateFormat", "yyyy-MM-dd HH:mm:ss.s")
        //connectionProperties.put("timestampFormat","yyyy-MM-dd HH:mm:ss.SSS")

        this.loadJDBC(connectUri, sourceQuery, connectionProperties)
      }
      case "HIVE" => {

        val sourceQuery: String = Util.getStringFromMap(mapDatasetInfo, PrepConst.SOURCE_QUERY)

        this.loadHive(sourceQuery)
      }
    }

    df
  }


  def loadFile( format:String, path:String, options:Map[String, String] = Map.empty[String, String]): DataFrame = {
    spark.read.format(format).options(options).load(path)

  }

  def loadHive( sourceQuery: String): DataFrame = {
    spark.sql(sourceQuery)
  }

  def loadJDBC( url: String, query: String,  connProp: java.util.Properties): DataFrame = {

    spark.read.jdbc(url, query , connProp)

  }

  /**
    * 설정 파일 및 이름 필요
    * @param df
    * @param snapshotInfo
    */
  def save(df: DataFrame, snapshotInfo: Map[String, Object]): Unit = {

    val ssType = Util.getStringFromMap(snapshotInfo, PrepConst.SS_TYPE,"HDFS")
    val ssFormat = Util.getStringFromMap(snapshotInfo, PrepConst.SS_FORMAT, "CSV")
    val ssCompression = Util.getStringFromMap(snapshotInfo, PrepConst.SS_COMPRESSION,"NONE")
    val ssMode = Util.getStringFromMap(snapshotInfo, PrepConst.SS_MODE, "OVERWRITE")

    val saveMode = if( ssMode == "OVERWRITE" ) SaveMode.Overwrite else SaveMode.Append

    ssType match {
      case "FILE" => {

        val baseDir= Util.getStringFromMap(snapshotInfo, PrepConst.SS_LOCAL_BASE_DIR)
        val filePath= "file://" +baseDir + "/" +Util.getStringFromMap(snapshotInfo, PrepConst.SS_NAME)
        val foramt = ssFormat.toLowerCase

        val options = if(  ssCompression != "NONE" ) {
          val delimiter = Util.getStringFromMap(snapshotInfo, PrepConst.DELIMITER)
          Map("compression" -> ssCompression.toLowerCase )
        } else {
          Map.empty[String, String];
        }

        this.saveFile(df, foramt, filePath, options, saveMode)
      }
      case "HDFS" => {

        val baseDir= Util.getStringFromMap(snapshotInfo, PrepConst.SS_STAGING_BASE_DIR)
        val filePath= baseDir + "/" +Util.getStringFromMap(snapshotInfo, PrepConst.SS_NAME)
        val foramt = ssFormat.toLowerCase

        val options = if(  ssCompression != "NONE" ) {
          val delimiter = Util.getStringFromMap(snapshotInfo, PrepConst.DELIMITER)
          Map("compression" -> ssCompression.toLowerCase )
        } else {
          Map.empty[String, String];
        }

        this.saveFile(df, foramt, filePath, options, saveMode)
      }
      case "HIVE" => {

        val tableName = Util.getStringFromMap( snapshotInfo, PrepConst.SS_NAME, "crime")

        this.saveHive(df, tableName, saveMode)
      }
      case "JDBC" => {

        //ORACLE,MYSQL,POSTGRESQL,HIVE, PRESTO, TIBERO

        val implementor = Util.getStringFromMap(snapshotInfo, PrepConst.IMPLEMENTOR)
        val sourceQuery = Util.getStringFromMap(snapshotInfo, PrepConst.SOURCE_QUERY)
        val connectUri  = Util.getStringFromMap(snapshotInfo, PrepConst.CONNECT_URI)
        val username    = Util.getStringFromMap(snapshotInfo, PrepConst.USERNAME)
        val password    = Util.getStringFromMap(snapshotInfo, PrepConst.PASSWORD)
        val tableName = Util.getStringFromMap( snapshotInfo, PrepConst.SS_NAME, "crime")

        val connectionProperties = new java.util.Properties()
        connectionProperties.put("driver", this.getDbDriver(implementor))
        connectionProperties.put("user", username)
        connectionProperties.put("password", password)

        this.saveJDBC(df, connectUri, tableName, connectionProperties, saveMode)

      }
    }

  }

  def saveHive(df: DataFrame, tableName: String , saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    df.write.mode(saveMode).saveAsTable(tableName);

  }


  def saveFile(df: DataFrame, format:String, path:String, options:Map[String, String] = Map.empty[String, String],
                                                                   saveMode: SaveMode = SaveMode.Overwrite): Unit = {


    df.write.format(format).mode(saveMode)
      .options(options).save(path)

  }

  def saveJDBC( df: DataFrame, url: String, tableName: String, connProp: java.util.Properties,
                                                              saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write.mode(saveMode).jdbc(url, tableName , connProp)

  }


  def createView(df: DataFrame, viewName: String) = {
    //spark.catalog.dropGlobalTempView(viewName)
    //df.createGlobalTempView(viewName)

    spark.catalog.dropTempView(viewName)
    df.createTempView(viewName)
    df
  }

  def registerUdfs() = {
    registerUdfReplace()
  }

  def registerUdfReplace() = {
    spark.udf.register("replace", (str: String, from: String, to: String, quote: String) => {
      var resultStr = ""

      if (quote == null) {
        resultStr = str.replace(from, to)
      } else {
        var inQuote = false

        var offsets = ArrayBuffer[Int](0)
        var offset = -1

        // find all occurrences of quote
        while ( {
          offset = str.indexOf(quote, offsets.last + 1); offset
        } > 0) {
          offsets += offset
        }
        println(offsets)

        // put together, replace only when not enclosed by quotes
        for (i <- 0 until offsets.size) {
          if (i == offsets.size - 1) {
            // if quote not closed, then do not replace
            resultStr += str.substring(offsets.apply(i)).replace(from, to)
          } else if (inQuote) {
            resultStr += str.substring(offsets.apply(i), offsets.apply(i + 1))
            inQuote = false
          } else {
            resultStr += str.substring(offsets.apply(i), offsets.apply(i + 1)).replace(from, to)
            inQuote = true
          }
        }
      }

      resultStr
    })
  }


}

