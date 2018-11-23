package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.parser.preparation.rule.expr.{Constant, Expression, Identifier}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.types.{ArrayType, MapType}

case class PrepUnnest(rule: Rule) extends PrepRule(rule) {
  val unnestRule = rule.asInstanceOf[Unnest]
  val targetColName = unnestRule.getCol
  val into = unnestRule.getInto
  val idx = unnestRule.getIdx

  val isArray = if( into.equalsIgnoreCase("ARRAY") ) true else false;


  override def transform(df: DataFrame): DataFrame = {

    val fieldNames = df.schema.fieldNames;

    if( !fieldNames.contains(targetColName) ) {
      throw new IllegalArgumentException("PrepUnnest: column not found: " + targetColName)
    }


    if( isArray && !isArrayType(df, targetColName) ) {
      throw new IllegalArgumentException("PrepUnnest:  not Array Type: " + targetColName)
    }

    if( !isArray && !isMapType(df, targetColName) ) {
      throw new IllegalArgumentException("PrepUnnest:  not Map Type: " + targetColName)
    }

    val colIndex = this.getColumnIndex(df, targetColName);

    var newColName = ""
    var newDf = df;
    if( isArray ){
      val index = this.getArrayIndex(idx)
      newColName = this.getNewColName(fieldNames.toList, index)

      newDf = newDf.withColumn(newColName, col(targetColName).getItem(index) )

    }else {
      val keyName = this.getMapKeyName(idx)
      newColName = this.getNewColName(fieldNames.toList, keyName)

      newDf = newDf.withColumn(newColName, col(targetColName)(keyName) )
    }

    val (firstNames,lastNames) = fieldNames.splitAt( colIndex+1)

    val headerName = firstNames.head;
    val selectColNames = firstNames.tail.toList ::: List(newColName) ::: lastNames.toList

    newDf.select(headerName, selectColNames: _*)

  }


  def isArrayType(df: DataFrame,  index : Int): Boolean = {
    this.getColumnType(df, index).isInstanceOf[ArrayType]
  }

  def isArrayType(df: DataFrame,  name : String): Boolean = {
    this.getColumnType(df, name).isInstanceOf[ArrayType]
  }

  def isMapType(df: DataFrame,  index : Int): Boolean = {
    this.getColumnType(df, index).isInstanceOf[MapType]
  }

  def isMapType(df: DataFrame,  name : String): Boolean = {
    this.getColumnType(df, name).isInstanceOf[MapType]
  }

  def getArrayIndex( index: Expression): Int = index match {
    case exp: Constant.StringExpr     => Integer.valueOf( exp.getEscapedValue)
    case exp: Constant.LongExpr       => exp.getValue().asInstanceOf[Long].intValue()
    case _                            => {
      throw new IllegalArgumentException("PrepUnnest: invalid index type: " + index.toString())
    }
  }

  def getMapKeyName( index: Expression): String = index match {
    case exp: Constant.StringExpr         => exp.getEscapedValue
    case exp: Identifier.IdentifierExpr   => {
      throw new IllegalArgumentException("PrepUnnest: idx on MAP type should be STRING (maybe, this is a column name): "
                  + exp.getValue())
    }
    case _                            => {
      throw new IllegalArgumentException("PrepUnnest: idx on MAP type should be STRING: " + index.toString())
    }
  }

  def getNewColName( colNameList: List[String], index: Int): String = {
    modifyDuplicatedColName(colNameList, "unnest_" + index)
  }

  def getNewColName( colNameList: List[String], key: String): String = {
    modifyDuplicatedColName(colNameList, "unnest_" + key)
  }

}
