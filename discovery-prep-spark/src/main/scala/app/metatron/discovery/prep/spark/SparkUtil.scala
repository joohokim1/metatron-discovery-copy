package app.metatron.discovery.prep.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object SparkUtil {
  val spark = Main.getSparkSession()

  def createView(df: DataFrame, viewName: String) = {
    spark.catalog.dropGlobalTempView(viewName)
    df.createGlobalTempView(viewName)
    df
  }

  def registerUdfs() = {
    registerUdfReplace()
  }

  def registerUdfReplace() = {
    spark.udf.register("replace", (str: String, from: String, to: String, quote: String) => {
      var resultStr = ""

      if (quote == null) {
        resultStr = str.replace(from, to)
      } else {
        var inQuote = false

        var offsets = ArrayBuffer[Int](0)
        var offset = -1

        // find all occurrences of quote
        while ( {
          offset = str.indexOf(quote, offsets.last + 1); offset
        } > 0) {
          offsets += offset
        }
        println(offsets)

        // put together, replace only when not enclosed by quotes
        for (i <- 0 until offsets.size) {
          if (i == offsets.size - 1) {
            // if quote not closed, then do not replace
            resultStr += str.substring(offsets.apply(i)).replace(from, to)
          } else if (inQuote) {
            resultStr += str.substring(offsets.apply(i), offsets.apply(i + 1))
            inQuote = false
          } else {
            resultStr += str.substring(offsets.apply(i), offsets.apply(i + 1)).replace(from, to)
            inQuote = true
          }
        }
      }

      resultStr
    })
  }
}
