package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import org.apache.spark.sql.{DataFrame, Row}

case class PrepHeader(rule: Rule) extends PrepRule(rule) {
  val header = rule.asInstanceOf[Header]
  val rownum = header.getRownum

  /*
  override def transform(df: DataFrame): DataFrame = {
    var newDf = df

    // TODO: Assuming the 1st row in DataFrame is the 1st line of the CSV might not work. Check it.
    val newColNames = df.collect.map(_.toSeq).apply(0).map(_.toString)

    for (i <- newDf.columns.indices) {
      newDf = newDf.withColumnRenamed(newDf.columns.apply(i), newColNames.apply(i))
    }

    newDf.except(df.limit(1))
  }
  */

  override def transform(df: DataFrame): DataFrame = {

    var newDf = if( rownum == 1) {
      this._header( df, df.head )
    } else {
      this._header( df, df.take(rownum.toInt).reverse.head )
    }

    newDf
  }

  private def _header( df: DataFrame, row: Row): DataFrame = {

    val structType = df.schema;
    val fieldNames = structType.fieldNames;

    var newDf = df;

    structType.foreach( structField => {

      val oldName = structField.name
      val index = fieldNames.indexOf(oldName)

      val newName = row.get(index).toString

      newDf = newDf.withColumnRenamed(oldName, newName)
    })

    newDf.filter( _ != row )

  }

}
