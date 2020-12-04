package mck.qb.columbia.feature.detect.reliability

import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object RelEngineClaimFeaturesJob extends Job {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.FET_REL_ENG_CLM)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  override def run(): Unit = {

    //this function takes in a dataframe and gives a data frame. we will be unioning two data frames and
    // a proper order of columns is very important. Thus, we created this function.
    def aggregateReliabilityColumns(ds: Dataset[Row]): DataFrame = {
      ds.groupBy("REL_NORMALIZED_COVERAGE", "REL_ENGINE_SERIAL_NUM", "REL_FAILURE_DATE").agg(
        first("REL_ENGINE_SERIAL_NUM").as("ESN"),
        min("REL_FAILURE_DATE").as("FAILURE_DATE"),
        first("REL_IN_SERVICE_DATE").as("CMP_IN_SERVICE_DATE"),
        first("REL_ENGINE_MILES").as("CMP_ENGINE_MILES"),
        first("REL_WORK_HOUR").as("CMP_ENGINE_WORK_HOUR"),
        collect_list("REL_CLAIM_NUM").as("CMP_CLAIM_NUM_LIST"),
        collect_list("REL_FAIL_CODE").as("CMP_FAIL_CODE_LIST"),
        collect_list("REL_FAILURE_DATE").as("CMP_FAIL_DATE_LIST"),
        collect_list("REL_FAILURE_PART").as("CMP_FAILURE_PART_LIST"),
        collect_list("REL_ABO_FAILURE_PART").as("CMP_ABO_FAILURE_PART_LIST"),
        collect_list("REL_DEALER_NAME").as("CMP_DEALER_NAME_LIST"),
        collect_list("REL_CLAIM_DATE").as("CMP_CLAIM_DATE_LIST"),
        collect_list("REL_FAILURE_PART_NUMBER").as("CMP_PART_NUM_LIST"),
        collect_list("REL_BASE_OR_ATS").as("CMP_BASE_OR_ATS_LIST"),
        collect_list("REL_PAID_OPEN").as("CMP_PAID_OPEN_LIST"),
        collect_list("REL_DATA_PROVIDER").as("CMP_DATA_PROVIDER_LIST"),
        collect_list("REL_FAILURE_CAUSE").as("CMP_CAUSE_LIST"),
        collect_list("REL_COMPLAINT").as("CMP_COMPLAINT_LIST"),
        collect_list("REL_CORRECTION").as("CMP_CORRECTION_LIST"),
        collect_list("REL_FAILURE_MODE").as("REL_CMP_FAILURE_MODE_LIST"),
        sum("REL_NET_AMOUNT").as("CMP_SUM_NET_AMOUNT"),
        sum("REL_LABOR_HOURS").as("CMP_SUM_LABOR_HOURS")
      ).
        withColumnRenamed("REL_NORMALIZED_COVERAGE", "CMP_COVERAGE").
        select("ESN",
          "CMP_COVERAGE",
          "FAILURE_DATE",
          "CMP_IN_SERVICE_DATE",
          "CMP_ENGINE_MILES",
          "CMP_ENGINE_WORK_HOUR",
          "CMP_CLAIM_NUM_LIST",
          "CMP_FAIL_CODE_LIST",
          "CMP_FAILURE_PART_LIST",
          "CMP_ABO_FAILURE_PART_LIST",
          "CMP_DEALER_NAME_LIST",
          "CMP_CLAIM_DATE_LIST",
          "CMP_PART_NUM_LIST",
          "CMP_BASE_OR_ATS_LIST",
          "CMP_PAID_OPEN_LIST",
          "CMP_DATA_PROVIDER_LIST",
          "CMP_CAUSE_LIST",
          "CMP_COMPLAINT_LIST",
          "CMP_CORRECTION_LIST",
          "CMP_FAIL_DATE_LIST",
          "REL_CMP_FAILURE_MODE_LIST",
          "CMP_SUM_NET_AMOUNT",
          "CMP_SUM_LABOR_HOURS"
        )
    }

    val engineClaimDetailDF: DataFrame = spark.sql(
      s"""
         |SELECT ESN_NEW AS ESN,
         |CLAIM_NUMBER,
         |DEALER_NAME,
         |FAILURE_PART_NUMBER,
         |MILEAGE,
         |Work_Hour,
         |FAILURE_PART,
         |ABO_FAILURE_PART,
         |BASE_OR_ATS,
         |COVERAGE,
         |TOTAL_COST,
         |LABOR_HOUR,
         |DATA_PROVIDER,
         |FAIL_CODE_MODE,
         |FAILURE_DATE,
         |CLAIM_SUBMIT_DATE,
         |VEHICLE_PURCHASE_DATE,
         |PAID_OPEN,
         |PAY_DATE,
         |Complaint,
         |Failure_Cause,
         |Correction,
         |FAILURE_MODE
         |FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_CLM_DTL_TBL)}
        """.stripMargin)
      .withColumnRenamed("ESN", "ENGINE_SERIAL_NUM")
      .withColumnRenamed("Claim_Number", "claim_num")
      .withColumnRenamed("Mileage", "engine_miles")
      .withColumnRenamed("Total_Cost", "net_amount")
      .withColumnRenamed("Labor_Hour", "labor_hours")
      .withColumnRenamed("Fail_Code_Mode", "fail_code")
      .withColumnRenamed("vehicle_purchase_date", "in_service_date")
      .withColumn("failure_date", to_date($"failure_date"))
      .withColumn("claim_date", to_date($"claim_submit_date"))


    val claimsProgDF = Util.formatCapitalizeNamesWithPrefix(engineClaimDetailDF, "REL_").
      withColumn("Base_or_ATS_Normal", Util.getNormbaseats($"REL_Base_or_ATS"))

    val replaceNullsDF = claimsProgDF.
      withColumn("REL_NORMALIZED_COVERAGE", Util.getNormCoverage($"REL_COVERAGE")).
      drop("REL_COVERAGE").
      withColumn("REL_FAILURE_DATE_new", coalesce($"REL_FAILURE_DATE", lit(new java.sql.Date(0)))).drop("REL_FAILURE_DATE").withColumnRenamed("REL_FAILURE_DATE_new", "REL_FAILURE_DATE").
      withColumn("REL_CLAIM_DATE_new", coalesce($"REL_CLAIM_DATE", lit(new java.sql.Date(0)))).drop("REL_CLAIM_DATE").withColumnRenamed("REL_CLAIM_DATE_new", "REL_CLAIM_DATE").
      withColumn("REL_IN_SERVICE_DATE_new", coalesce($"REL_IN_SERVICE_DATE", lit(new java.sql.Date(0)))).drop("REL_IN_SERVICE_DATE").withColumnRenamed("REL_IN_SERVICE_DATE_new", "REL_IN_SERVICE_DATE").
      withColumn("REL_ENGINE_MILES_new", coalesce($"REL_ENGINE_MILES", lit(0L.toLong))).drop("REL_ENGINE_MILES").withColumnRenamed("REL_ENGINE_MILES_new", "REL_ENGINE_MILES").
      withColumn("REL_DEALER_NAME_new", coalesce($"REL_DEALER_NAME", lit("0".toString()))).drop("REL_DEALER_NAME").withColumnRenamed("REL_DEALER_NAME_new", "REL_DEALER_NAME")

    val allDf = replaceNullsDF.
      where("REL_NORMALIZED_COVERAGE in ('Warranty','Policy','Campaign_TRP','Field_test','Emission')")

    val dsidClaimFeaturesDF = aggregateReliabilityColumns(allDf)

//todo: use Util.formatCapitalizeNamesWithPrefix to rename
    val dsidClaimFeaturesFinalDF = dsidClaimFeaturesDF.
      withColumnRenamed("ESN", "REL_ESN").
      withColumnRenamed("CMP_COVERAGE", "REL_CMP_COVERAGE").
      withColumnRenamed("FAILURE_DATE", "REL_FAILURE_DATE").
      withColumnRenamed("CMP_IN_SERVICE_DATE", "REL_CMP_IN_SERVICE_DATE").
      withColumnRenamed("CMP_ENGINE_MILES", "REL_CMP_ENGINE_MILES").
      withColumnRenamed("CMP_ENGINE_WORK_HOUR", "REL_CMP_ENGINE_WORK_HOUR").
      withColumnRenamed("CMP_CLAIM_NUM_LIST", "REL_CMP_CLAIM_NUM_LIST").
      withColumnRenamed("CMP_FAIL_CODE_LIST", "REL_CMP_FAIL_CODE_LIST").
      withColumnRenamed("CMP_FAILURE_PART_LIST", "REL_CMP_FAILURE_PART_LIST").
      withColumnRenamed("CMP_ABO_FAILURE_PART_LIST", "REL_CMP_ABO_FAILURE_PART_LIST").
      withColumnRenamed("CMP_DEALER_NAME_LIST", "REL_CMP_DEALER_NAME_LIST").
      withColumnRenamed("CMP_CLAIM_DATE_LIST", "REL_CMP_CLAIM_DATE_LIST").
      withColumnRenamed("CMP_PART_NUM_LIST", "REL_CMP_PART_NUM_LIST").
      withColumnRenamed("CMP_BASE_OR_ATS_LIST", "REL_CMP_BASE_OR_ATS_LIST").
      withColumnRenamed("CMP_PAID_OPEN_LIST", "REL_CMP_PAID_OPEN_LIST").
      withColumnRenamed("CMP_DATA_PROVIDER_LIST", "REL_CMP_DATA_PROVIDER_LIST").
      withColumnRenamed("CMP_CAUSE_LIST","REL_CMP_CAUSE_LIST").
      withColumnRenamed("CMP_COMPLAINT_LIST","REL_CMP_COMPLAINT_LIST").
      withColumnRenamed("CMP_CORRECTION_LIST","REL_CMP_CORRECTION_LIST").
      withColumnRenamed("CMP_SUM_NET_AMOUNT", "REL_CMP_SUM_NET_AMOUNT").
      withColumnRenamed("CMP_FAIL_DATE_LIST", "REL_CMP_FAIL_DATE_LIST"). //====================fw
      withColumnRenamed("CMP_SUM_LABOR_HOURS", "REL_CMP_SUM_LABOR_HOURS")

    // filter out records that have lists of length > 1000--probably data errors and causes Hive to crash
    val resultDF = Util.formatCapitalizeNames(dsidClaimFeaturesFinalDF.where(size($"REL_CMP_FAIL_DATE_LIST") < 1000))

    //Writing to a table.
    Util.saveData(resultDF, cfg(Constants.FET_DB), cfg(Constants.REL_CLM_FET_TBL), cfg(Constants.HDFS_PATH))
  }

}
