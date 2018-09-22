package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import org.apache.spark.sql.DataFrame

case class PrepHeader(rule: Rule) extends PrepRule(rule) {
  val header = rule.asInstanceOf[Header]
  val rownum = header.getRownum

  override def transform(df: DataFrame): DataFrame = {
    var newDf = df

    // TODO: Assuming the 1st row in DataFrame is the 1st line of the CSV might not work. Check it.
    val newColNames = df.collect.map(_.toSeq).apply(0).map(_.toString)

    for (i <- newDf.columns.indices) {
      newDf = newDf.withColumnRenamed(newDf.columns.apply(i), newColNames.apply(i))
    }

    newDf.except(df.limit(1))
  }
}
