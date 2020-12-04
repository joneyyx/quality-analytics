package mck.qb.library

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.
      builder().
      master("local").
      appName("spark test example").
      getOrCreate()
  }

}
