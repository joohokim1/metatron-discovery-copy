package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.spark.SparkUtil
import org.apache.spark.sql.DataFrame

case class PrepKeep(rule: Rule) extends PrepRule(rule) {
  val keep = rule.asInstanceOf[Keep]
  val row = keep.getRow

  override def transform(df: DataFrame): DataFrame = {
    SparkUtil.createView(df, "temp")

    spark.sql("SELECT * FROM global_temp.temp WHERE " + row.toString.replace("==", "="))  // FIXME: replace() will be deleted after ensuring UI doesn't use (and == will be unparsable)
  }
}
