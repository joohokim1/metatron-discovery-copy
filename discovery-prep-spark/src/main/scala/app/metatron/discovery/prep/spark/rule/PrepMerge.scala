package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws}

case class PrepMerge(rule: Rule) extends PrepRule(rule) {
  val mergeRule = rule.asInstanceOf[Merge]
  val colExp = mergeRule.getCol
  val withStr = mergeRule.getWith.replaceAll("'","")
  val newColName = mergeRule.getAs.replaceAll("'","")
  val targetColNames = getIdentifierList(colExp)


  override def transform(df: DataFrame): DataFrame = {

    if (targetColNames.length < 1) {
      throw new IllegalArgumentException("DfMerge.prepare(): no input column designated")
    }

    val fieldNames = df.schema.fieldNames;

    var colIndex = this. getLastColIndex( df, targetColNames) // max index

    val modifyNewColName = modifyDuplicatedColName(fieldNames.toList, newColName)

    var newDf= df.withColumn(modifyNewColName,concat_ws(withStr, targetColNames.map(c => col(c)): _*))

    val (firstNames,lastNames) = fieldNames.splitAt( colIndex+1)

    val headerName = firstNames.head;
    val selectColNames = firstNames.tail.toList ::: List(modifyNewColName) ::: lastNames.toList

    newDf.select(headerName, selectColNames: _*)

  }

}
