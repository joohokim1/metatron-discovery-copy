package app.metatron.discovery.prep.spark

import org.apache.spark.sql.DataFrame

trait Transformable {
  def transform(df: DataFrame) : DataFrame
}
