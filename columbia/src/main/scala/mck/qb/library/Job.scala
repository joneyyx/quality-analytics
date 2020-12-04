package mck.qb.library

import mck.qb.library.sample.SuperJob
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait Job extends SuperJob{

  //val logger = Logger.getLogger(this.getClass)

  implicit val spark = Util.getSpark

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  sc.setLogLevel("WARN")

  override def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()

    logger.log(Level.WARN, s"Starting Job.")
    // Try catch here
    run()
    logger.log(Level.WARN, s"Finished Job in ${(System.currentTimeMillis() - start)/1000} seconds.")

  }

  /**
    * Intended to be implemented for every job created.
    */
  def run()

  override def superrun(spark: SparkSession): Unit = {}


}
