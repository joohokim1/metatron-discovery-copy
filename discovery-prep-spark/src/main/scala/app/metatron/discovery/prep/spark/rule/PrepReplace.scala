package app.metatron.discovery.prep.spark.rule

import java.util.regex.{Matcher, Pattern}

import app.metatron.discovery.prep.parser.exceptions.{FunctionColumnNotFoundException, RuleException}
import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser

import scala.collection.JavaConverters._
import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.parser.preparation.rule.expr._
import app.metatron.discovery.prep.spark.rule.util.{ConstNumericBinding, DataFrameRowNumericBinding, NullNumericBinding, RuleRegexToken}
import app.metatron.discovery.prep.spark.{RegexUtil, SparkUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{callUDF, col, lit, struct, udf}

case class PrepReplace(rule: Rule, ruleString: String) extends PrepRule(rule) {
  val replace = rule.asInstanceOf[Replace]
  val colExp = replace.getCol
  val on = replace.getOn
  val withExp = replace.getWith
  val quote = replace.getQuote
  val global = replace.getGlobal
  val ignoreCase: Boolean = if( replace.getIgnoreCase != null) replace.getIgnoreCase else false
  val rowExp = replace.getRow // 있으면 사용시 항상 replaceValCol을 이용해서 $col을 치환해야 한다

  val patternStr = this.getPatternString()
  val pattern = this.getPattern(patternStr)
  val quoteStr = this.getQuoteStr()
  val withExpr = withExp.asInstanceOf[Expr]
  val targetColNames = getIdentifierList(colExp)

  var rowExprMap = this.getReplaceRowMap()

  val originalQuoteStr = this.getOriginalQuoteStr();

  override def transform(df: DataFrame): DataFrame = {
    if (!colExp.isInstanceOf[Identifier.IdentifierExpr] && !colExp.isInstanceOf[Identifier.IdentifierArrayExpr]) {
      throw new IllegalArgumentException("PrepReplace.transform: wrong target column expression: " + colExp.toString)
    }

    for( name <- targetColNames) {
      if( !this.isStringType(df, name)) {
        throw new IllegalArgumentException("PrefReplace: works only on STRING: " + name)
      }
    }

    def makeReplaceUdf(colName: String, pattern: Pattern, withExpr: Expr, orgQuoteStr: String, isGlobal: Boolean, rowExpr: Expr) = {
      val isQuote = orgQuoteStr != null && orgQuoteStr != "";

      udf((targetStr: String, dfRow: Row) => {
        // println("colName:"+ colName);
        // println("row:" + dfRow)
        if (targetStr == null) {
          targetStr
        } else {

          val binding = new DataFrameRowNumericBinding(dfRow)

          val checkCond = this.checkCondition(rowExpr, binding)

          var quoteCount = 0;
          if (isQuote ) {
            quoteCount = StringUtils.countMatches(targetStr, orgQuoteStr) % 2
          }

          if (!checkCond) {
            targetStr
          } else {

            if (!isQuote || quoteCount == 0) {
              val matcher = pattern.matcher(targetStr);
              if (matcher.find()) {
                if (isGlobal) {
                  matcher.replaceAll(withExpr.eval(binding).stringValue());
                } else {
                  matcher.replaceFirst(withExpr.eval(binding).stringValue());
                }
              } else {
                targetStr
              }
            } else {
              val lastIndex = targetStr.lastIndexOf(orgQuoteStr);
              val targetStr2 = targetStr.substring(lastIndex);
              val newTargetStr = targetStr.substring(0, lastIndex);

              val matcher = pattern.matcher(newTargetStr);
              if (matcher.find()) {
                if (isGlobal) {
                  matcher.replaceAll(withExpr.eval(binding).stringValue()) + targetStr2;
                } else {
                  matcher.replaceFirst(withExpr.eval(binding).stringValue()) + targetStr2;
                }
              } else {
                newTargetStr
              }
            }
          } //end of chechCond else

        } //end of else
      })
    }

    val fieldNames = df.schema.fieldNames;
    var existColNames = targetColNames.filter(colName => fieldNames.contains(colName))

    val ruleRegexToken = new RuleRegexToken(ruleString);
    val containsColumns = ruleRegexToken.getColumnNames(List("with", "row"), fieldNames)

    existColNames.foldLeft(df) {
      (tempDf, colName) => {
        // remove itself
        val needNames = containsColumns - colName;

        tempDf.withColumn(colName,
          makeReplaceUdf(colName, pattern, withExpr, originalQuoteStr, global, rowExprMap(colName))
          (col(colName), struct(colName, needNames.toList: _*))
        )

      }
    }
  }

  def getPattern(patternStr: String): Pattern = {

    RegexUtil.getPattern(patternStr, quote, ignoreCase)

  }

  def getPatternString(): String = {
    RegexUtil.getPatternString(on, quote, ignoreCase, "PrepReplace Role on")
  }

  def getQuoteStr(): String = {
    RegexUtil.getQuoteStr(quote, "PrepReplace Rule quote")
  }

  def getOriginalQuoteStr(): String = {
    RegexUtil.getOriginalQuoteStr(quote, "PrepReplace Rule quote")
  }

  def getReplaceRowMap(): Map[String, Expr] = {
    var states = scala.collection.mutable.Map[String, Expr]()

    for (colName <- targetColNames) {
      val rule = new RuleVisitorParser().parse(ruleString)
      val rowExpr = rule.asInstanceOf[Replace].getRow
      RegexUtil.replaceValCol(rowExpr, colName)
      states(colName) = rowExpr.asInstanceOf[Expr]

    }

    states.toMap
  }
}