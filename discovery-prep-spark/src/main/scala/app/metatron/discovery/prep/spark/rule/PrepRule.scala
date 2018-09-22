package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule.Rule
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.{IdentifierArrayExpr, IdentifierExpr}
import app.metatron.discovery.prep.spark.Main
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class PrepRule(rule: Rule) {
  val spark = Main.getSparkSession()

  // should be overriden
  def transform(df: DataFrame) : DataFrame = df

  // Utility functions
  def getIdentifierList(expr: Expression): List[String] = {
    var buf = ListBuffer[String]()

    if (expr.isInstanceOf[IdentifierExpr]) {
      buf += expr.asInstanceOf[IdentifierExpr].getValue
    } else if (expr.isInstanceOf[IdentifierArrayExpr]) {
      buf ++= expr.asInstanceOf[IdentifierArrayExpr].getValue.asScala
    }

    buf.toList
  }
}
