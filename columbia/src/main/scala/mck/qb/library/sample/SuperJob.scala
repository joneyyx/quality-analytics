package mck.qb.library.sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SuperJob {

  val logger = Logger.getLogger(this.getClass)





  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()

    logger.log(Level.WARN, s"Starting Job.")
    // Try catch here
    val spark=SparkSession.builder().enableHiveSupport().getOrCreate()
    superrun(spark)
    logger.log(Level.WARN, s"Finished Job in ${(System.currentTimeMillis() - start)/1000} seconds.")

  }

  /**
    * Intended to be implemented for every job created.
    */
  def superrun(spark:SparkSession)


}