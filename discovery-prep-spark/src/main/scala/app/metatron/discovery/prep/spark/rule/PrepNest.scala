package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, lit, map}

case class PrepNest(rule: Rule) extends PrepRule(rule) {
  val nestRule = rule.asInstanceOf[Nest]
  val colExp = nestRule.getCol
  val into = nestRule.getInto
  val newColName = nestRule.getAs.replaceAll("'","")
  val targetColNames = getIdentifierList(colExp)

  val isArray = if( into.equalsIgnoreCase("ARRAY") ) true else false;


  override def transform(df: DataFrame): DataFrame = {

    if (targetColNames.length < 1) {
      throw new IllegalArgumentException("PrepNest.prepare(): no input column designated")
    }

    if (!colExp.isInstanceOf[Identifier.IdentifierExpr] && !colExp.isInstanceOf[Identifier.IdentifierArrayExpr]) {
      throw new IllegalArgumentException("PrepNest.transform: wrong target column expression: " + colExp.toString)
    }

    val fieldNames = df.schema.fieldNames;

    var colIndex = this. getLastColIndex( df, targetColNames) // max index

    val modifyNewColName = modifyDuplicatedColName(fieldNames.toList, newColName)

    var existColNames = targetColNames.filter( colName => fieldNames.contains(colName) )

    var newDf = df;
    if( this.isArray) {
      newDf = newDf.withColumn(modifyNewColName, array(existColNames.map(c => col(c)): _*))
    } else {
      newDf = newDf.withColumn(modifyNewColName, map(existColNames.flatMap(c => List(lit(c), col(c)) ): _*))
    }

    val (firstNames,lastNames) = fieldNames.splitAt( colIndex+1)

    val headerName = firstNames.head;
    val selectColNames = firstNames.tail.toList ::: List(modifyNewColName) ::: lastNames.toList

    newDf.select(headerName, selectColNames: _*)

  }

}
