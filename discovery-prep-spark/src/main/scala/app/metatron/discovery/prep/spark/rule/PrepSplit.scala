package app.metatron.discovery.prep.spark.rule

import java.util.regex.Pattern

import app.metatron.discovery.prep.parser.preparation.rule._
import app.metatron.discovery.prep.spark.RegexUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame}


case class PrepSplit(rule: Rule) extends PrepRule(rule) {
  val splitRule = rule.asInstanceOf[Split]
  val targetColName = splitRule.getCol
  val on = splitRule.getOn
  val quote = splitRule.getQuote
  val ignoreCase: Boolean = if( splitRule.getIgnoreCase != null) splitRule.getIgnoreCase else false;
  val limit: Int = if( splitRule.getLimit != null) splitRule.getLimit else 0

  val patternStr = this.getPatternString()
  val pattern = this.getPattern(patternStr)
  val quoteStr = this.getQuoteStr()

  val originalQuoteStr = this.getOriginalQuoteStr();


  override def transform(df: DataFrame): DataFrame = {

    val fieldNames = df.schema.fieldNames;

    if( !fieldNames.contains(targetColName) ) {
      throw new IllegalArgumentException("PrepSplit: column not found: " + targetColName)
    }

    if( !this.isStringType(df, targetColName)) {
      throw new IllegalArgumentException("PrepSplit: works only on STRING: " + targetColName)
    }


    def makeSplitUdf(pattern: Pattern, orgQuoteStr: String, splitLimit: Int) = {
      val isQuote = orgQuoteStr != null && orgQuoteStr != "";

      udf( (targetStr: String) => {
        var tokens = new Array[String](splitLimit + 1)

        if(targetStr != null) {

          var quoteCount = 0;
          if (isQuote) {
            quoteCount = StringUtils.countMatches(targetStr, orgQuoteStr) % 2
          }

          if (!isQuote || quoteCount == 0) {
              val tempTokens: Array[String] = pattern.split (targetStr, splitLimit + 1)
              for ( i <- 0 to (tempTokens.length - 1)) {
                tokens(i) = tempTokens(i);
              }

          } else {
            val lastQuoteMark = targetStr.lastIndexOf(orgQuoteStr)
            val tempTokens = pattern.split(targetStr.substring(0, lastQuoteMark), splitLimit + 1)
            for ( i <- 0 to (tempTokens.length - 1)) {
              tokens(i) = tempTokens(i);
            }

            val lastIndex = tempTokens.length -1;
            tokens(lastIndex) = tokens(lastIndex) + targetStr.substring(lastQuoteMark)
          }

        } //end of else

        tokens;
      })
    }

    val colIndex = this.getColumnIndex(df, targetColName);
    val newColNames = this.getNewColNameList(fieldNames.toList, targetColName, limit+1)

    val tempColName = "temp_"

    var newDf= df.withColumn(tempColName,
      makeSplitUdf( this.pattern, this.originalQuoteStr, this.limit)(col(targetColName))
    )

    for ( i <- 0 to limit) {
      newDf = newDf.withColumn(newColNames(i), col(tempColName).getItem(i))
    }

    newDf = newDf.drop(tempColName);

    val (firstNames,lastNames) = fieldNames.splitAt( colIndex+1)

    val headerName = firstNames.head;
    val selectColNames = firstNames.tail.toList ::: newColNames ::: lastNames.toList

    newDf.select(headerName, selectColNames: _*)

  }

  def getPattern(patternStr: String): Pattern = {

    RegexUtil.getPattern(patternStr, quote, ignoreCase)

  }

  def getPatternString(): String = {

    RegexUtil.getPatternString(on, quote, ignoreCase, "PrepSplit Role on")

  }

  def getQuoteStr(): String = {
    RegexUtil.getQuoteStr(quote, "PrepSplit Rule quote")
  }


  def getOriginalQuoteStr(): String = {
    RegexUtil.getOriginalQuoteStr(quote, "PrepSplit Rule quote")
  }

  def getNewColNameList( fieldNames: List[String], colName:String, count: Int): List[String] = {
    var newNames = new Array[String](count)

    for ( i <- 0 to (count - 1)) {
      newNames(i) =  modifyDuplicatedColName(fieldNames.toList, "split_" + colName + (i+1) )
    }

    newNames.toList
  }

}

// https://stackoverflow.com/questions/38104600/how-to-change-a-column-position-in-a-spark-dataframe
// https://stackoverflow.com/questions/39235704/split-spark-dataframe-string-column-into-multiple-columns