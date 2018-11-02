package app.metatron.discovery.prep.spark.rule.util

import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr

/**
  * Created by nowone on 2018. 10. 25..
  */
class ConstNumericBinding(value: Any) extends Expr.NumericBinding {
  override def get(name: String): Object = {
    if(value != null ) value.asInstanceOf[Object] else null
  }
}
