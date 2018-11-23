package app.metatron.discovery.prep.spark.rule.util

import java.sql.Timestamp

import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr
import org.apache.spark.sql.Row
import org.joda.time.DateTime

/**
  * Created by nowone on 2018. 10. 25..
  */
class DataFrameRowNumericBinding(row: Row) extends Expr.NumericBinding {
  override def get(name: String): Object = {

    val colName = name.replaceAll("\"","")
    val value = if( row == null) null else row.getAs[Object](colName)

    if( value !=null && value.isInstanceOf[Timestamp]) {
      new DateTime(value)
    }else{
      value
    }
  }
}
