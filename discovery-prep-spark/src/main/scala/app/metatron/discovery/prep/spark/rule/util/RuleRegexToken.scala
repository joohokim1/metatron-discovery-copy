package app.metatron.discovery.prep.spark.rule.util

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by nowone on 2018. 10. 31..
  */
class RuleRegexToken (ruleString: String) {

  val ruleTokenSet = Set("col","on","after","before","global","with",
    "ignoreCase","quote","row","to","type","value","as",
    "into","rownum","idx","order","group","groupEvery",
    "leftSelectCol","rightSelectCol","condition","joinType","dataSet2"
  )

  val ruleRegex: Regex = ("("+ruleTokenSet.mkString(":{1}|")+":{1})").r
  val tokenMap = parse();

  def parse(): Map[String, String] = {
    val firstSpaceIndex = ruleString.indexOf(' ');
    val ruleName = ruleString.substring(0, firstSpaceIndex ).trim

    var resultMap = scala.collection.mutable.Map[String, String]()
    resultMap("ruleName") = ruleName;

    var startIndex  = -1;
    var key = "";

    for (patternMatch <- ruleRegex.findAllMatchIn(ruleString)) {
      // println(s"key: ${patternMatch.group(1)}")

      if( startIndex != -1 && !resultMap.contains(key)  ){
        resultMap(key) = ruleString.substring(startIndex, patternMatch.start ) ;
      }

      val originToken = patternMatch.group(1);
      startIndex = patternMatch.start + originToken.length;
      key = originToken.substring(0, originToken.length -1).trim;
    }

    if( startIndex != -1 && !resultMap.contains(key) ) {
      resultMap(key) = ruleString.substring(startIndex );
    }

    println(resultMap.toMap)
    resultMap.toMap
  }


  def isNeedCol(argName: String, colName: String): Boolean = {
    if( !tokenMap.contains(argName)) {
      false
    }else {
      val value = tokenMap(argName)
      if (value.indexOf("$col") != -1 || value.indexOf(colName) != -1) {
        true;
      } else {
        false
      }
    }

  }

  def isNeedRow(argName: String, fieldNames: Array[String], colName: String = null ): Boolean = {

    if( !tokenMap.contains(argName) || fieldNames == null) {
      false
    }else {

      val value = tokenMap(argName)

      var names = fieldNames;
      if (colName != null) {
        // colName 있으면 검사에제 제외  (Except colName)
        names = fieldNames.filter(_ != colName)
      }

      var result = false;
      for (name <- names) {
        if (result == false && value.indexOf(name) != -1) {
          result = true;
        }
      }

      result;
    }
  }

  /**
    * Expression 필요한 컬럼 이름 추출
    * @param argNames
    * @param fieldNames
    * @return
    */
  def getColumnNames(argNames: List[String], fieldNames: Array[String]): Set[String] ={
    var columNames = new ListBuffer[String]()

    argNames.foreach( (argName) => {
      if( tokenMap.contains(argName) ) {
        val value = tokenMap(argName);

        fieldNames.foreach( (fName) => {
          if( value.indexOf(fName) != -1) {
            columNames += fName;
          }
        })
      }
    })

    return columNames.distinct.toSet

  }

}
