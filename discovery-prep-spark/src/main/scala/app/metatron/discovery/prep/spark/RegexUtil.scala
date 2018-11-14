package app.metatron.discovery.prep.spark

import java.util.regex.Pattern

import scala.collection.JavaConverters._
import app.metatron.discovery.prep.parser.preparation.rule.expr._
import app.metatron.discovery.prep.spark.rule.util.DataFrameRowNumericBinding
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Created by nowone on 2018. 10. 26..
  */
object RegexUtil {

  /**
    * replace $col with colName;
    * @param expr
    * @param colName
    */
  def replaceValCol(expr: Expression, colName: String): Unit = {

    def replaceCol(exprInner: Expression, colNameInner: String): Unit = exprInner match {
      case identExpr: Identifier.IdentifierExpr     => {
        if(identExpr.getValue().equals("$col")){
          identExpr.setValue(colNameInner)
        }
      }
      case opExpr: Expr.BinaryNumericOpExprBase     => {
        replaceCol(opExpr.getLeft, colNameInner)
        replaceCol(opExpr.getRight, colNameInner)
      }
      case funExpr: Expr.FunctionExpr               => {
        val args = funExpr.getArgs().asScala;
        for (arg <- args) {
          replaceCol(arg, colNameInner);
        }
      }
      case _                                        => { /* Nothing */ }
    } //end of replaceCal

    if( expr != null && colName != null){
      replaceCol( expr, colName);
    }

  }

  def disableRegexSymbols(str: String): String = {
    val regExSymbols = "[\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)\\?\\*\\+\\.\\>]"
    str.replaceAll(regExSymbols, "\\\\$0")
  }

  def makeCaseInsensitive(str: String): String = {
    var ignorePatternStr = ""
    for( ch <- str){
      val chStr = ch.toString;
      if (chStr.matches("[a-zA-Z]")) ignorePatternStr += "[" + chStr.toUpperCase + chStr.toLowerCase + "]"
      else ignorePatternStr += ch
    }

    ignorePatternStr
  }

  def compilePatternWithQuote(patternStr: String, quoteStr: String): String = {
    patternStr + "(?=([^" + quoteStr + "]*" + quoteStr + "[^" + quoteStr + "]*" + quoteStr + ")*[^" + quoteStr + "]*$)"
  }

  def countMatches(targetStr: String, quoteStr: String): Int = StringUtils.countMatches(targetStr, quoteStr)

  def getPattern(patternStr: String, quote:Expression, ignoreCase:Boolean): Pattern = {
    if( quote == null) {

      if (ignoreCase != null && ignoreCase)
        Pattern.compile(patternStr, Pattern.LITERAL + Pattern.CASE_INSENSITIVE)
      else
        Pattern.compile(patternStr, Pattern.LITERAL)

    } else {
      Pattern.compile(patternStr)
    }
  }

  def getPatternString(on:Expression, quote:Expression, ignoreCase:Boolean, ruleName:String ="Rule"): String = {
    if( quote == null) {
      on match {
        case expr: Constant.StringExpr => expr.getEscapedValue
        case expr: RegularExpr => expr.getEscapedValue
        case _  =>  throw new IllegalArgumentException(ruleName+ ": illegal pattern type: " + on.toString)
      }

    } else {
      val onStr = on match {
        case expr: Constant.StringExpr => {
          var onStrTemp = expr.getEscapedValue
          onStrTemp = disableRegexSymbols(onStrTemp);

          if (ignoreCase != null && ignoreCase)
            onStrTemp = makeCaseInsensitive(onStrTemp)

          onStrTemp
        }
        case expr: RegularExpr => expr.getEscapedValue
        case _  =>  throw new IllegalArgumentException(ruleName +": illegal pattern type: " + on.toString)
      }

      val quoteStr = getQuoteStr(quote, ruleName +" quote")

      compilePatternWithQuote(onStr, quoteStr)
    }

  }

  def getQuoteStr(quote:Expression, ruleName:String ="Rule quote"): String = {
    quote match {
      case expr: Constant.StringExpr => disableRegexSymbols(expr.getEscapedValue)
      case expr: RegularExpr => expr.getEscapedValue
      case null              => null
      case _  =>  throw new IllegalArgumentException(ruleName +": illegal pattern type: " + quote.toString)
    }

  }

  def getOriginalQuoteStr(quote:Expression, ruleName:String ="Rule quote"): String = {
    quote match {
      case expr: Constant.StringExpr => expr.getEscapedValue
      case expr: RegularExpr => expr.getEscapedValue
      case null              => null
      case _  =>  throw new IllegalArgumentException(ruleName +": illegal pattern type: " + quote.toString)
    }

  }

}
