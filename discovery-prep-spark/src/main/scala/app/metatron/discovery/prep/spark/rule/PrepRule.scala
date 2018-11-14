package app.metatron.discovery.prep.spark.rule

import app.metatron.discovery.prep.parser.exceptions.FunctionColumnNotFoundException
import app.metatron.discovery.prep.parser.preparation.rule.Rule
import app.metatron.discovery.prep.parser.preparation.rule.expr.{Expr, ExprEval, Expression}
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.{IdentifierArrayExpr, IdentifierExpr}
import app.metatron.discovery.prep.spark.rule.util.DataFrameRowNumericBinding
import app.metatron.discovery.prep.spark.SparkUtil
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.annotation.tailrec
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
    this.getColumnType(df, colName).isInstanceOf[StringType]
  }

  def isStringType(df: DataFrame,  index : Int): Boolean = {
    this.getColumnType(df, index).isInstanceOf[StringType]
  }


  def checkCondition( expr: Expr, dfRow: Row): Boolean = {
    val binding = new DataFrameRowNumericBinding(dfRow)
    this.checkCondition(expr, binding)
  }

  def checkCondition( expr: Expr, binding: Expr.NumericBinding): Boolean = {
    if (expr == null) {
      true
    } else {

      val exprEval = this.getEval(expr, binding)

      if( exprEval == null) {
        false
      }else {
        exprEval.asBoolean()
      }

    }
  }


  def makeParsable(colName: String): String = {
    var newColName = colName;
    if (colName.matches("^\'.+\'")) {
      newColName = colName.substring(1, colName.length - 1)
    }
    colName.replaceAll("[\\p{Punct}\\p{IsPunctuation} ]", "_")
  }

  def assertParsable(colName: String) {
    assert(makeParsable(colName) == colName, colName)
  }

  def modifyDuplicatedColName(colNameList: List[String], colName: String): String = {
    assertParsable(colName)

    val newColNamelist = colNameList.map( _.toLowerCase())

    if ( !newColNamelist.contains(colName.toLowerCase)) {
      colName
    } else {

      val maxIndex = 1000 // check max Index

      @tailrec
      def checkAndNewName(checkName: String, index: Int): String = {
        if( newColNamelist.contains(checkName)) {

          if( index > maxIndex ){
            // TODO change throw ?
            assert(false, colName)
          }

          checkAndNewName("%s_%d".format(colName, index), index)
        }else{
          checkName
        }
      }

      checkAndNewName("%s_%d".format(colName, 1), 1);
    }

  }

  def getLastColIndex (df: DataFrame, targetColNames: List[String]): Int = {
    var colIndex = -1 // max index

    for( name <- targetColNames ) {
      val index = this.getColumnIndex(df, name);

      if( index > colIndex) {
        colIndex = index;
      }
    }

    colIndex
  }

  def getEval(expr: Expr, dfRow: Row): ExprEval = {
    val binding = new DataFrameRowNumericBinding(dfRow)
    this.getEval(expr, binding)
  }

  def getEval(expr: Expr, binding: Expr.NumericBinding): ExprEval = {
    try {
      expr.eval(binding)
    } catch {
      case e: FunctionColumnNotFoundException => throw e
      case e: NullPointerException => throw e
      case _ => null;
    }
  }

}
