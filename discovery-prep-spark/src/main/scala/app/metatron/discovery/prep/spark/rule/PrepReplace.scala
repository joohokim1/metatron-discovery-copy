package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.spark.SparkUtil
import org.apache.spark.sql.DataFrame

case class PrepReplace(rule: Rule) extends PrepRule(rule) {
  val replace = rule.asInstanceOf[Replace]
  val col = replace.getCol
  val on = replace.getOn
  val withExpr = replace.getWith
  val quote = replace.getQuote
  val global = replace.getGlobal
  val ignoreCase = replace.getIgnoreCase

  override def transform(df: DataFrame): DataFrame = {
    var outColStr: String = ""
    val quoteStr = if (quote == null) null else quote.toString
    val targetColNames = getIdentifierList(col)

    SparkUtil.createView(df, "temp")

    for (colName <- df.columns) {
      if (targetColNames.contains(colName)) {
        outColStr += "replace(`%s`, %s, %s, %s) AS `%s`, ".format(colName, on.toString, withExpr.toString, quoteStr, colName)
      } else {
        outColStr += "`%s`, ".format(colName)
      }
    }
    outColStr = outColStr.substring(0, outColStr.length - 2)  // remove ", "

    spark.sql("SELECT %s FROM global_temp.temp".format(outColStr))
  }
}
