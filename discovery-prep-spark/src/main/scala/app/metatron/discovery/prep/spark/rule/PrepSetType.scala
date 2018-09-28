package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.spark.SparkUtil
import org.apache.spark.sql.DataFrame

case class PrepSetType(rule: Rule) extends PrepRule(rule) {
  val settype = rule.asInstanceOf[SetType]
  val col = settype.getCol
  val typeStr = settype.getType

  def sparkTypeOf(typeStr: String) = typeStr.toLowerCase match {
    case "long" => "long"
    case "double" => "double"
    case "string" => "string"
    case _ => println(f"invalid typeStr: $typeStr"); "string"
  }

  override def transform(df: DataFrame): DataFrame = {
    var outColStr: String = ""
    val targetColNames = getIdentifierList(col)

    SparkUtil.createView(df, "temp")

    for (colName <- df.columns) {
      if (targetColNames.contains(colName)) {
        outColStr += "cast(`%s` AS %s) AS %s, ".format(colName, sparkTypeOf(typeStr), colName)
      } else {
        outColStr += "`%s`, ".format(colName)
      }
    }
    outColStr = outColStr.substring(0, outColStr.length - 2)  // remove ", "

    var newDf = spark.sql("SELECT %s FROM global_temp.temp".format(outColStr))
    newDf.show()
    newDf
  }
}
