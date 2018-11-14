package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class PrepSetType(rule: Rule) extends PrepRule(rule) {
  val settype = rule.asInstanceOf[SetType]
  val col = settype.getCol
  val typeStr = settype.getType
  val foramtStr = settype.getFormat

  def getDataTypeByName( typeName : String): Option[DataType]  = typeName.toLowerCase match {
    case "string"       => Some(StringType)
    case "boolean"      => Some(BooleanType)
    case "byte"         => Some(ByteType)
    case "short"        => Some(ShortType)
    case "int"          => Some(IntegerType)
    case "integer"      => Some(IntegerType)
    case "long"         => Some(LongType)
    case "float"        => Some(FloatType)
    case "double"       => Some(DoubleType)
    case "date"         => Some(DateType)
    case "timestamp"    => Some(TimestampType)
   // case "array"        => Some(org.apache.spark.sql.types.ArrayType)
   // case "map"          => Some(org.apache.spark.sql.types.MapType)
    case _              => Some(StringType)

    // TODO 추가 ?
  }

  override def transform(df: DataFrame): DataFrame = {

    if (col == null || typeStr == null) {
      // exception or progress 선택
      return df;
    }

    val targetColNames = getIdentifierList(col)

    val dataTypeOpt = this.getDataTypeByName(typeStr);

    if( dataTypeOpt.isEmpty){
      return df;
    }

    val dataType = dataTypeOpt.get;

    val fieldNames = df.schema.fieldNames;

    var existColNames = targetColNames.filter( colName => fieldNames.contains(colName) )

    var newDf = df;
    for (colName <- existColNames) {
      val index = fieldNames.indexOf(colName)
      val typeName = newDf.schema(index).dataType.typeName

      if ( "timestamp" == typeStr && typeName == "string" && foramtStr != null){
        // 문자열이고 포멧이 있으면 ...
        newDf = newDf.withColumn(colName, unix_timestamp(df(colName),foramtStr).cast(dataType) );
      }else {
        newDf = newDf.withColumn(colName, df(colName).cast(dataType) )
      }

    }

    newDf
  }
}

/*
  def sparkTypeOf(typeStr: String) = typeStr.toLowerCase match {
    case "long" => "long"
    case "double" => "double"
    case "string" => "string"
    case _ => println(f"invalid typeStr: $typeStr"); "string"
  }

  override def transform(df: DataFrame): DataFrame = {
    var outColStr: String = ""
    val targetColNames = getIdentifierList(col)

    for (colName <- df.columns) {
      if (targetColNames.contains(colName)) {
        outColStr += "cast(`%s` AS %s) AS %s, ".format(colName, sparkTypeOf(typeStr), colName)
      } else {
        outColStr += "`%s`, ".format(colName)
      }
    }
    outColStr = outColStr.substring(0, outColStr.length - 2)  // remove ", "

    SparkUtil.createView(df, "temp")

    val sqlStr = "SELECT %s FROM temp".format(outColStr);
    println(sqlStr)
    spark.sql(sqlStr)
  }
  */