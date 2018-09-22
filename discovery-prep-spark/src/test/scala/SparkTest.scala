package app.metatron.discovery.prep.spark

import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class SparkTest extends FunSuite with Serializable {
  val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

  def getDfCrime: DataFrame = {
    val filePath = "src/test/resources/crime.csv"
    spark.read.format("csv").option("header", "false").load(filePath)
  }

  def getDfQuoteTest: DataFrame = {
    val filePath = "src/test/resources/quote_test.csv"
    spark.read.format("csv").option("header", "true").load(filePath)
  }

  def createView(df: DataFrame, viewName: String) = {
    spark.catalog.dropGlobalTempView(viewName)
    df.createGlobalTempView(viewName)
    df
  }

  def header(df: DataFrame) : DataFrame = {
    var newDf = df

    // TODO: Assuming the 1st row in DataFrame is the 1st line of the CSV might not work. Check it.

    val newColNames = df.collect.map(_.toSeq).apply(0).map(_.toString)

    for (i <- newDf.columns.indices) {
      if (newDf.columns.apply(i) != "monotonically_increasing_id") {
        newDf = newDf.withColumnRenamed(newDf.columns.apply(i), newColNames.apply(i))
      }
    }

    newDf.except(df.limit(1))
  }

  def headerKeepOrder(df: DataFrame) : DataFrame = {
    val df: DataFrame = getDfCrime.withColumn("monotonically_increasing_id", monotonically_increasing_id)
    header(df).sort("monotonically_increasing_id").drop("monotonically_increasing_id")
  }

  def replace(df: DataFrame, targetColNames: List[String], from: String, to: String) : DataFrame = {
    var outColStr: String = ""
    var newDf = df

    for (colName <- newDf.columns) {
      if (targetColNames.contains(colName)) {
        outColStr += "translate(`%s`, '%s', '%s') AS `%s`, ".format(colName, from, to, colName)
      } else {
        outColStr += "`%s`, ".format(colName)
      }
    }
    outColStr = outColStr.substring(0, outColStr.length - 2)  // remove ", "

    createView(spark.sql("SELECT %s FROM global_temp.crime".format(outColStr)), "crime")
  }

  test("SparkTest.show") {
    val df: DataFrame = getDfCrime
    assert(df !== null)

    df.show()
  }

  test("SparkTest.header") {
    val df: DataFrame = header(getDfCrime)
    df.show()
  }

  // Spark DataFrame do not guarantee the order of rows.
  // If you really want to keep the order, then you must sort explicitly, using SORT rules.
  // We don't have any plan for sorting under the cover, which will makes more accidents.
  // Instead, there'll be a function that adds a line-number column.
  // This test-case was just for feasibility checking, and the answer was NOPE.
  test("SparkTest.headerKeepOrder") {
    headerKeepOrder(getDfCrime).show()
  }

  test("SparkTest.global_temp") {
    val df: DataFrame = createView(getDfCrime, "crime")
    val newDf = spark.sql("SELECT * FROM global_temp.crime LIMIT 1")
    newDf.show()
  }

  // crime.csv does not have space-included column names anymore.
  // It's to proceed other tests smoothly.
  // When quoting on column names becomes enable, this test-case will be resurrected.
//  test("SparkTest.various_colnames_in_SQL") {
//    var newDf = createView(header(getDfCrime), "crime")
//    newDf = spark.sql("SELECT `Total Crime` FROM global_temp.crime")
//    newDf.show()
//  }

  test("SparkTest.replace") {
    var newDf = createView(header(getDfCrime), "crime")

    val targetColNames = List[String]("Population_", "Total_Crime", "Violent_Crime", "Property_Crime", "Murder_", "Forcible_Rape_", "Robbery_", "Aggravated_Assault_", "Burglary_", "Larceny_Theft_", "Vehicle_Theft_")

    newDf = replace(newDf, targetColNames, "_", "")
    newDf = replace(newDf, targetColNames, " ", "")
    newDf = replace(newDf, targetColNames, ",", "")

    newDf.show()
  }

  def replace(str: String, from: String, to: String, quote: String): String = {
    var quoteOffsets = ArrayBuffer[Int]()
    for (i <- 0 until str.length) {
      if (str.substring(i, quote.size) == quote) {
        quoteOffsets += i
      }
    }

    var resultStr = str.substring(0, quoteOffsets.apply(0)).replace(from, to)
    var skip = true
    for (i <- quoteOffsets.indices) {
      if (skip) {
        resultStr += str.substring(i, quoteOffsets.apply(0))
        skip = false
      } else {
        resultStr += str.substring(i, quoteOffsets.apply(0)).replace(from, to)
        skip = true
      }
    }

    resultStr
  }

  test("SparkTest.replaceWithQoute") {
    var newDf = createView(getDfQuoteTest, "quote_test")
    newDf.show(truncate = false)

    spark.udf.register("replace", (str: String, from: String, to: String, quote: String) => {
      var resultStr = ""
      var inQuote = false

      var offsets = ArrayBuffer[Int](0)
      var offset = -1

      // find all occurrences of quote
      while ({offset = str.indexOf(quote, offsets.last + 1); offset} > 0) {
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

      resultStr
    })

    spark.sql("""SELECT replace(`B`, ' ', '_', '"') FROM global_temp.quote_test""").show(truncate = false)
  }
}

