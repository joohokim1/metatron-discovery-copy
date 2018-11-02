package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.exceptions.FunctionColumnNotFoundException
import app.metatron.discovery.prep.parser.preparation.rule.Rule
import app.metatron.discovery.prep.parser.preparation.rule.expr.{Expr, Expression, Identifier}
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.{IdentifierArrayExpr, IdentifierExpr}
import app.metatron.discovery.prep.spark.rule.util.DataFrameRowNumericBinding
import app.metatron.discovery.prep.spark.{Main, SparkUtil}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, StringType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class PrepRule(rule: Rule) extends Serializable {
  val spark = SparkUtil.getSession()

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

  def isColumnExist(df: DataFrame, colName: String): Boolean = {
    val structType = df.schema;

    structType.fieldNames.contains(colName)
  }

  def getColumnName(df: DataFrame,  index : Int): String = {
    val structType = df.schema;

    structType.fieldNames(index)
  }

  def getColumnIndex(df: DataFrame,  colName : String): Int = {
    val structType = df.schema;

    structType.fieldNames.indexOf(colName)
  }

  def getColumnType(df: DataFrame,  colName : String): DataType = {
    val structType = df.schema;

    structType(colName).dataType;

  }

  def getColumnType(df: DataFrame,  index : Int): DataType = {
    val structType = df.schema;

    structType(index).dataType;

  }

  def isStringType(df: DataFrame,  colName : String): Boolean = {
    StringType == this.getColumnType(df, colName)
  }

  def isStringType(df: DataFrame,  index : Int): Boolean = {
    StringType == this.getColumnType(df, index)
  }

  def checkCondition(expr: Expr, row: Row): Boolean ={

     if( expr ==  null) {
       true
     } else {
        try{
          expr.eval(new DataFrameRowNumericBinding(row)).asBoolean();
        } catch {
          case e: FunctionColumnNotFoundException => throw e;
          case e: NullPointerException => throw e
          case _: Throwable => false
        }
     }

  }
}
