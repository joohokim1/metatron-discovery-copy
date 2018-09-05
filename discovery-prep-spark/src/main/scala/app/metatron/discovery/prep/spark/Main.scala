package app.metatron.discovery.prep.spark

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    println("Hello, Spark!")

    val rule = new RuleVisitorParser().parse("rename col: x to: y")
    println(rule)
  }
}
