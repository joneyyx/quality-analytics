package mck.qb.library

import org.apache.spark.sql.SparkSession

trait BaseJob {

  val spark = Util.getSpark

  val sc = spark.sparkContext

  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    createRaw()

    createPrimary()

    createFeatures()
  }

  def createRaw () : Unit = {
    // In general this won't be implemented
  }

  def createPrimary()

  def createFeatures()


}
