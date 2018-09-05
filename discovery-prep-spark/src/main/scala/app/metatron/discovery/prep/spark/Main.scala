package app.metatron.discovery.prep.spark

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    println("Hello, Spark!")
  }
}
