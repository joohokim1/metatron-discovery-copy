package app.metatron.discovery.prep.spark.rule.util

import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr
import org.apache.spark.sql.Row

/**
  * Created by nowone on 2018. 10. 25..
  */
class NullNumericBinding extends Expr.NumericBinding {
  override def get(name: String): Object = {
    null
  }
}
