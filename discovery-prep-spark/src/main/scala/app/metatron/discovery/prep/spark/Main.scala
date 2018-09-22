package app.metatron.discovery.prep.spark

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object Main {
  var spark: SparkSession = null
  var transformer: Transformer = null

  def getSparkSession() = {   // TODO: use perperties from polaris
    if (spark == null) {
      spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    }
    spark
  }

  def main(args: Array[String]): Unit = {
    transformer = new Transformer(getSparkSession())
    transformer.process(args)
  }

  def javaCall(args: java.util.List[String]) = {
    if (transformer == null) {
      transformer = new Transformer(getSparkSession())
    }
    transformer.process(args.asScala.toArray)
  }
}
