package app.metatron.discovery.prep.spark
import org.apache.spark.sql.DataFrame

class Rename(pcol: String, pto: String) extends Transformable {
  var col: String = pcol
  var to: String = pto

  override def transform(df: DataFrame): DataFrame = {
    df
  }
}
