package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import org.apache.spark.sql.DataFrame

case class PrepDrop(rule: Rule) extends PrepRule(rule) {
  val drop = rule.asInstanceOf[Drop]
  val col = drop.getCol

  override def transform(df: DataFrame): DataFrame = {

    val colNames = getIdentifierList(col)

    var newDf = df

    for ( colName <- colNames) {
      newDf = newDf.drop(colName)
    }

    newDf
  }
}
