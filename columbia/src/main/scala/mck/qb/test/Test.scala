package mck.qb.test

import mck.qb.library.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * xxx
  * creat by newforesee 2019-07-02
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("dsdsd").master("local").getOrCreate()

    spark.sql("show databases").show

    val df0 = spark.table("qa_prd_report.om_stats")
    val df2019 = df0.filter("BUILD_YEAR=2019").cache()
    val dfall = df2019.filter("REL_QUARTER_BUILD_DATE='All'")
    val dfq1 = df2019.filter("REL_QUARTER_BUILD_DATE='2019-Q1'")
    val dfq2 = df2019.filter("REL_QUARTER_BUILD_DATE='2019-Q2'")

    val dfq1q2 = dfq1.union(dfq2)


    dfq1q2.count()
    dfq1q2.distinct().count()
    val schema: StructType = dfq1q2.schema

    val rddsubtract = dfq1q2.drop("REL_QUARTER_BUILD_DATE").rdd.subtract(dfall.drop("REL_QUARTER_BUILD_DATE").rdd)

    rddsubtract.count()


  }

}
