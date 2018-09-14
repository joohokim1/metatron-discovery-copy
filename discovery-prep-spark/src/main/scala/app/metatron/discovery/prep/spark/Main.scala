package app.metatron.discovery.prep.spark

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object Main {
  var transformer: Transformer = null

  def getSparkSession() = {   // TODO: use perperties from polaris
    SparkSession.builder().config("spark.master", "local").getOrCreate()
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
