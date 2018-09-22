package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression
import org.apache.spark.sql.DataFrame

case class PrepRename(rule: Rule) extends PrepRule(rule) {
  val rename = rule.asInstanceOf[Rename]
  val col: Expression = rename.getCol
  val to: Expression = rename.getTo

  override def transform(df: DataFrame): DataFrame = {
    val colNames = getIdentifierList(col)
    val toNames = getIdentifierList(to)
    var newDf = df

    for (i <- colNames.indices) {
      newDf = newDf.withColumnRenamed(colNames(i), toNames(i))
    }

    newDf
  }
}
