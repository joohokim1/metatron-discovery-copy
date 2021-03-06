package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr
import app.metatron.discovery.prep.spark.rule.util.DataFrameRowNumericBinding
import org.apache.spark.sql.DataFrame

case class PrepKeep(rule: Rule) extends PrepRule(rule)  {
  val keep = rule.asInstanceOf[Keep]
  val row = keep.getRow
  val condExpr = row.asInstanceOf[Expr]

  override def transform(df: DataFrame): DataFrame = {


    if( row.isInstanceOf[Expr.BinAsExpr]) {
      throw new Exception( condExpr.toString())
    }

    df.filter( dfRow => {
      val binding = new DataFrameRowNumericBinding(dfRow)
      condExpr.eval(binding).longValue() == 1
    })

    /*
    SparkUtil.createView(df, "temp")
    spark.sql("SELECT * FROM global_temp.temp WHERE " + row.toString.replace("==", "="))  // FIXME: replace() will be deleted after ensuring UI doesn't use (and == will be unparsable)
    */
  }

}
