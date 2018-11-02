package app.metatron.discovery.prep.spark

import scala.collection.JavaConverters._


object Main {

  var transformer: Transformer = null

  def main(args: Array[String]): Unit = {

    var argsInfo = args;

    /*
    if( args.length < 1) {
      argsInfo = this.getTestInfo();
    }
    */

    SparkUtil.getSession(argsInfo(0));

    transformer = new Transformer(SparkUtil.getSession())
    transformer.process(argsInfo)

  }

  def javaCall(args: java.util.List[String]) = {
    if (transformer == null) {
      transformer = new Transformer(SparkUtil.getSession())
    }
    transformer.process(args.asScala.toArray)
  }

}
