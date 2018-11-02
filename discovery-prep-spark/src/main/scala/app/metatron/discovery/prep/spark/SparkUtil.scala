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
    val ssFormat = Util.getStringFromMap(mapDatasetInfo, PrepConst.SS_FORMAT, "CSV")

    val df = importType match {
      case "FILE" => {

        val filePath: String = Util.getStringFromMap(mapDatasetInfo, PrepConst.FILE_PATH)

        /*
        val options = if(  mapDatasetInfo.get(PrepConst.DELIMITER).isDefined) {
          val delimiter = Util.getStringFromMap(mapDatasetInfo, PrepConst.DELIMITER)
          // Map("header" -> "false", "delimiter" -> delimiter)
          Map("header" -> "false", "sep" -> delimiter, "quote"-> "\"")
        } else {
          Map("header" -> "false")
        }
        */

        //val options = Map("header" -> "true", "sep" -> ",", "quote"-> "'","ignoreLeadingWhiteSpace" -> "true", "escape" ->"\\")

        var optionsMap = scala.collection.mutable.Map[String, String]()

        mapDatasetInfo.keys.foreach( (key) =>

          if ( PrepConst.DELIMITER == key) {
           val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.DELIMITER)
           if( value != null  ){
             if( value.length == 1 ){
               optionsMap("sep") = value
             }else if( value.length > 1){
               throw new IllegalArgumentException("spark cvs delimiter(sep): need the single character: length:" + value.length)
             }
           }

          } else if ( PrepConst.HEADER == key) {
            val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.HEADER)
            if (value != null && "true" == value.toLowerCase) {
              optionsMap("header") = "true"
            }

          } else if ( PrepConst.QUOTE == key) {
            val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.QUOTE)
            if( value != null  ){
              if( value.length == 0 || value.length == 1 ){
                //If you would like to turn off quotations, you need to set not null but an empty string.
                optionsMap("quote") = value
              }else if( value.length > 1){
                throw new IllegalArgumentException("spark cvs quote: need the single character: length:" + value.length)
              }
            }
          } else if ( PrepConst.ESCAPE == key) {
            val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.ESCAPE)
            if (value != null) {
              if (value.length == 1) {
                optionsMap("escape") = value
              } else if (value.length > 1) {
                throw new IllegalArgumentException("spark cvs escape: need the single character: length:" + value.length)
              }
            }
          } else if ( PrepConst.MULTILINE == key) {
            val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.MULTILINE)
            if (value != null && "true" == value.toLowerCase) {
              optionsMap("multiLine") = "true"
            }
          } else if ( PrepConst.IGNORE_LEADING_WHITESPACE == key) {
            val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.IGNORE_LEADING_WHITESPACE)
            if (value != null && "true" == value.toLowerCase) {
              optionsMap("ignoreLeadingWhiteSpace") = "true"
            }
          } else if ( PrepConst.IGNORE_TRAILING_WHITESPACE == key) {
            val value = Util.getStringFromMap(mapDatasetInfo, PrepConst.IGNORE_TRAILING_WHITESPACE)
            if (value != null && "true" == value.toLowerCase) {
              optionsMap("ignoreTrailingWhiteSpace") = "true"
            }
          }
        )


        //val options = Map("header" -> "false", "sep" -> ",", "quote"-> "\"","ignoreLeadingWhiteSpace" -> "true")
        //val options = Map("header" -> "true", "sep" -> ",", "quote"-> "'","ignoreLeadingWhiteSpace" -> "true", "escape" ->"\\")

        //spark.read.format("com.databricks.spark.csv") different

        this.loadFile(ssFormat, filePath, optionsMap.toMap)
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
    //println(options)
    spark.read.format(format).options(options).load(path)

  }

  def loadHive( sourceQuery: String): DataFrame = {
    spark.sql(sourceQuery)
    // beeline -u jdbc:hive2://localhost:10000/default -n scott -w password_file
  }

  def loadJDBC( url: String, query: String,  connProp: java.util.Properties): DataFrame = {

    spark.read.jdbc(url, query , connProp)

  }

  def getFormat( ftype: String): String = ftype match {
    case "CSV"      => "csv"
    case "JSON"     => "json"
    case "ORC"      => "orc"
    case "PARQUET"  => "parquet"
    case _          => "text"
  }

  def getCompression( ctype: String): String = ctype match {
    case "SNAPPY"   => "snappy"
    case "ZLIB"     => "zlib"
    case "LZO"      => "lzo"
    case _          => ""
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
        val foramt = this.getFormat(ssFormat)

        var optionsMap = scala.collection.mutable.Map[String, String]()

        snapshotInfo.keys.foreach( (key) =>

          if ( PrepConst.DELIMITER == key) {
            val value = Util.getStringFromMap(snapshotInfo, PrepConst.DELIMITER)
            if( value != null  ){
              if( value.length == 1 ){
                optionsMap("sep") = value
              }else if( value.length > 1){
                throw new IllegalArgumentException("spark cvs delimiter(sep): need the single character: length:" + value.length)
              }
            }

          }else if ( PrepConst.SS_COMPRESSION == key) {
            val value = Util.getStringFromMap(snapshotInfo, PrepConst.SS_COMPRESSION,"NONE")
            if( value != null && "NONE" != value ){
              optionsMap("compression") -> this.getCompression(ssCompression)
            }

          } else if ( PrepConst.HEADER == key) {
            val value = Util.getStringFromMap(snapshotInfo, PrepConst.HEADER)
            if (value != null && "true" == value.toLowerCase) {
              optionsMap("header") = "true"
            }

          } else if ( PrepConst.QUOTE == key) {
            val value = Util.getStringFromMap(snapshotInfo, PrepConst.QUOTE)
            if( value != null  ){
              if( value.length == 0 || value.length == 1 ){
                //If you would like to turn off quotations, you need to set not null but an empty string.
                optionsMap("quote") = value
              }else if( value.length > 1){
                throw new IllegalArgumentException("spark cvs quote: need the single character: length:" + value.length)
              }
            }
          } else if ( PrepConst.ESCAPE == key) {
            val value = Util.getStringFromMap(snapshotInfo, PrepConst.ESCAPE)
            if (value != null) {
              if (value.length == 1) {
                optionsMap("escape") = value
              } else if (value.length > 1) {
                throw new IllegalArgumentException("spark cvs escape: need the single character: length:" + value.length)
              }
            }
          } else if ( PrepConst.QUOTE_ALL == key) {
            val value = Util.getStringFromMap(snapshotInfo, PrepConst.QUOTE_ALL)
            if (value != null && "true" == value.toLowerCase) {
              optionsMap("quoteAll") = "true"
            }
          }
        )


        this.saveFile(df, foramt, filePath, optionsMap.toMap, saveMode)
      }
      case "HDFS" => {

        val baseDir= Util.getStringFromMap(snapshotInfo, PrepConst.SS_STAGING_BASE_DIR)
        val filePath= baseDir + "/" +Util.getStringFromMap(snapshotInfo, PrepConst.SS_NAME)
        val foramt = ssFormat.toLowerCase

        val options = if(  ssCompression != "NONE" ) {
          val delimiter = Util.getStringFromMap(snapshotInfo, PrepConst.DELIMITER)
          Map("compression" -> this.getCompression(ssCompression) )
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
    spark.udf.register("replaceUDF", (str: String, from: String, to: String, quote: String) => {
      var resultStr = ""
      //str: column value from:pattern
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
        //println(offsets)

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

