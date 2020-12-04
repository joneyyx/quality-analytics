package mck.qb.columbia.feature.detect.om.base

import java.sql.Date

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.WrappedArray

abstract class DetectCreateIncidentsJob extends Job {

  import spark.implicits._

  def getEngType(): String

  def getTargetTableKey(): String

  val myConf: Config = AppConfig.getMyConfig(Constants.FET_DETECT_CRT_INC, logger)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString



  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {
    val telDf = Util.filterEnginesByType(cfg, readtelFinFeatDF(cfg), getEngType(), esnColName = "ESN")
    val reliabilityDf = Util.filterEnginesByType(cfg, readReliabilityFeaturesDf(cfg), getEngType(), esnColName = "REL_ESN")

    val aggregatedIncidentTrackerDF = buildIncidentDF(telDf, reliabilityDf)//.cache()

    Util.saveData(aggregatedIncidentTrackerDF, cfg(Constants.FET_DB), cfg(getTargetTableKey()), cfg(Constants.HDFS_PATH))
  }


  def readtelFinFeatDF(cfg: String => String) = {
    spark.sql(
      s"""SELECT * FROM ${cfg(Constants.FET_DB)}.${cfg(Constants.TEL_FNL_FET_TBL)}""".stripMargin
    )
  }

  def readReliabilityFeaturesDf(cfg: String => String) = {

    val strSql: String =
      s"""
         |SELECT
         | REL_ESN,
         |REL_CMP_COVERAGE,
         |REL_FAILURE_DATE,
         |REL_CMP_IN_SERVICE_DATE,
         |REL_CMP_ENGINE_MILES,
         |REL_CMP_ENGINE_WORK_HOUR,
         |REL_CMP_CLAIM_NUM_LIST,
         |REL_CMP_FAIL_CODE_LIST,
         |REL_CMP_FAILURE_PART_LIST,
         |REL_CMP_ABO_FAILURE_PART_LIST,
         |REL_CMP_DEALER_NAME_LIST,
         |REL_CMP_CLAIM_DATE_LIST,
         |REL_CMP_PART_NUM_LIST,
         |REL_CMP_BASE_OR_ATS_LIST,
         |REL_CMP_PAID_OPEN_LIST,
         |REL_CMP_DATA_PROVIDER_LIST,
         |REL_CMP_FAIL_DATE_LIST,
         |REL_CMP_CAUSE_LIST,
         |REL_CMP_COMPLAINT_LIST,
         |REL_CMP_CORRECTION_LIST,
         |REL_CMP_FAILURE_MODE_LIST,
         |REL_CMP_SUM_NET_AMOUNT,
         |REL_CMP_SUM_LABOR_HOURS
         | FROM ${cfg(Constants.FET_DB)}.${cfg(Constants.REL_CLM_FET_TBL)}
         | """.stripMargin

    val reliabilityFeaturesDf = spark.sql(strSql).
        filter(size($"REL_CMP_FAIL_DATE_LIST") < 100)
    reliabilityFeaturesDf
  }


  def buildIncidentDF(telFinFeatDF: DataFrame, reliabilityFeaturesDf: DataFrame) = {

    val telDateColumn = "FAILURE_DATE"
    val reliabilityDateColumn = "REL_FAILURE_DATE"

    // QUALITY_COMPUTE.REL_DSID_FEATURES

    val coreIncidentTracker: DataFrame = reliabilityFeaturesDf.join(telFinFeatDF, $"ESN" === $"REL_ESN" &&
      (
        col(reliabilityDateColumn).isNull || // TEL only
          col(telDateColumn).isNull || // Reliability only
          col(reliabilityDateColumn) === col(telDateColumn)), "outer")
        .persist(StorageLevel.MEMORY_AND_DISK)

    //val columnStrings = (coreIncidentTracker.columns.toList ::: Array("GROUP_BY_DATE", "GROUP_BY_ID").toList).toArray
    // val orderedColumns = columnStrings.map(col)

    val trackerWithArtificialColumns = coreIncidentTracker.
      withColumn("GROUP_BY_ID", when($"REL_ESN".isNotNull, $"REL_ESN").otherwise($"ESN")).
      withColumn("GROUP_BY_DATE", when(col(reliabilityDateColumn).isNotNull, col(reliabilityDateColumn)).otherwise(col(telDateColumn))).
      withColumn("INCIDENT_TRACKER_JOIN_TYPE",
        when(col(reliabilityDateColumn).isNull, lit("TEL_ONLY")).otherwise(
          when(col(telDateColumn).isNull, lit("RELIABILITY_ONLY")).otherwise(
            lit("TEL_AND_RELIABILITY")
          ))).
      // Create a struct with CLAIM_ID_SEQ and AMOUNTS.
      withColumn("REL_CMP_SUM_NET_AMOUNT", struct($"REL_CMP_CLAIM_NUM_LIST", $"REL_CMP_SUM_NET_AMOUNT")).
      withColumn("REL_CMP_SUM_LABOR_HOURS", struct($"REL_CMP_CLAIM_NUM_LIST", $"REL_CMP_SUM_LABOR_HOURS"))
    trackerWithArtificialColumns.persist(StorageLevel.MEMORY_AND_DISK)


    //val telIncidents = trackerWithArtificialColumns.filter($"INCIDENT_TRACKER_JOIN_TYPE" === "TEL_ONLY")
    //val reliabilityIncidents = trackerWithArtificialColumns.filter($"INCIDENT_TRACKER_JOIN_TYPE" === "RELIABILITY_ONLY")
    //val bothIncidents = trackerWithArtificialColumns.filter($"INCIDENT_TRACKER_JOIN_TYPE" === "TEL_AND_RELIABILITY")

    //assert(telIncidents.count() > 0)
    //assert(reliabilityIncidents.count() > 0)
    //assert(bothIncidents.count() > 0)
    //assert(telIncidents.count() + reliabilityIncidents.count() + bothIncidents.count() == coreIncidentTracker.count())

    val aggregatedIncidentTracker = trackerWithArtificialColumns.
      groupBy("GROUP_BY_ID", "GROUP_BY_DATE").agg(
      coalesce(first($"ESN"), first($"REL_ESN")) as "REL_ESN",
      min("REL_FAILURE_DATE") as "REL_FAILURE_DATE",
      min("FAILURE_DATE") as "TEL_FAILURE_DATE",
      first("Report_Type") as "Report_Type",
      first("REL_CMP_COVERAGE") as "REL_CMP_COVERAGE",
      first("REL_CMP_IN_SERVICE_DATE") as "REL_CMP_IN_SERVICE_DATE",
      collect_list("Occurrence_Date_Time_LIST") as "Occurrence_Date_Time_LIST",
      collect_list("Latitude_LIST") as "Latitude_LIST",
      collect_list("Longitude_LIST") as "Longitude_LIST",
      collect_list("Altitude_LIST") as "Altitude_LIST",
      collect_list("Fault_Code_LIST") as "Fault_Code_LIST",
      collect_list("Fault_Code_Description_LIST") as "Fault_Code_Description_LIST",
      collect_list("Derate_Flag_LIST") as "Derate_Flag_LIST",
      collect_list("Shutdown_Flag_LIST") as "Shutdown_Flag_LIST",
      collect_list("Fault_Code_Category_LIST") as "Fault_Code_Category_LIST",
      collect_list("nonIm_Occurrence_Date_Time_LIST") as "nonIm_Occurrence_Date_Time_LIST",
      collect_list("nonIm_Latitude_LIST") as "nonIm_Latitude_LIST",
      collect_list("nonIm_Longitude_LIST") as "nonIm_Longitude_LIST",
      collect_list("nonIm_Altitude_LIST") as "nonIm_Altitude_LIST",
      collect_list("nonIm_Fault_Code_LIST") as "nonIm_Fault_Code_LIST",
      collect_list("nonIm_Fault_Code_Description_LIST") as "nonIm_Fault_Code_Description_LIST",
      collect_list("nonIm_Derate_Flag_LIST") as "nonIm_Derate_Flag_LIST",
      collect_list("nonIm_Shutdown_Flag_LIST") as "nonIm_Shutdown_Flag_LIST",
      collect_list("nonIm_Fault_Code_Category_LIST") as "nonIm_Fault_Code_Category_LIST",
      collect_list("REL_CMP_ENGINE_MILES") as "REL_CMP_ENGINE_MILES_LIST",
      collect_list("REL_CMP_ENGINE_WORK_HOUR") as "REL_CMP_ENGINE_WORK_HOUR_LIST",
      collect_list("REL_CMP_CLAIM_NUM_LIST") as "REL_CMP_CLAIM_NUM_LIST",
      collect_list("REL_CMP_FAIL_CODE_LIST") as "REL_CMP_FAIL_CODE_LIST",
      collect_list("REL_CMP_FAILURE_PART_LIST") as "REL_CMP_FAILURE_PART_LIST",
      collect_list("REL_CMP_ABO_FAILURE_PART_LIST") as "REL_CMP_ABO_FAILURE_PART_LIST",
      collect_list("REL_CMP_DEALER_NAME_LIST") as "REL_CMP_DEALER_NAME_LIST",
      collect_list("REL_CMP_CLAIM_DATE_LIST") as "REL_CMP_CLAIM_DATE_LIST",
      collect_list("REL_CMP_PART_NUM_LIST") as "REL_CMP_PART_NUM_LIST",
      collect_list("REL_CMP_BASE_OR_ATS_LIST") as "REL_CMP_BASE_OR_ATS_LIST",
      collect_list("REL_CMP_PAID_OPEN_LIST") as "REL_CMP_PAID_OPEN_LIST",
      collect_list("REL_CMP_DATA_PROVIDER_LIST") as "REL_CMP_DATA_PROVIDER_LIST",
      collect_list("REL_CMP_FAIL_DATE_LIST").as("REL_CMP_FAIL_DATE_LIST"),
      collect_list("REL_CMP_CAUSE_LIST").as("REL_CMP_CAUSE_LIST"),
      collect_list("REL_CMP_COMPLAINT_LIST").as("REL_CMP_COMPLAINT_LIST"),
      collect_list("REL_CMP_CORRECTION_LIST").as("REL_CMP_CORRECTION_LIST"),
      collect_list("REL_CMP_FAILURE_MODE_LIST").as("REL_CMP_FAILURE_MODE_LIST"),
      collect_set("REL_CMP_SUM_NET_AMOUNT") as "REL_CMP_SUM_NET_AMOUNT",
      collect_set("REL_CMP_SUM_LABOR_HOURS") as "REL_CMP_SUM_LABOR_HOURS",
      collect_list("Mileage_LIST") as "Mileage_LIST",
      collect_list("nonIm_Mileage_LIST") as "nonIm_Mileage_LIST",
      collect_list("WORK_HOUR_LIST") as "WORK_HOUR_LIST",
      collect_list("nonIm_WORK_HOUR_LIST") as "nonIm_WORK_HOUR_LIST",
      collect_list("FAULT_CODE_REASON_LIST") as "FAULT_CODE_REASON_LIST",
      collect_list("NONIM_FAULT_CODE_REASON_LIST") as "NONIM_FAULT_CODE_REASON_LIST",
      collect_set("CALIBRATION_VERSION_LIST") as "CALIBRATION_VERSION_LIST",
      collect_set("nonIm_CALIBRATION_VERSION_LIST") as "nonIm_CALIBRATION_VERSION_LIST"

    ).withColumn("Occurrence_Date_Time_LIST", Util.flatTimestampMapStrSet($"Occurrence_Date_Time_LIST")).
      withColumn("Latitude_LIST", Util.flatMapDouble($"Latitude_LIST")).
      withColumn("Longitude_LIST", Util.flatMapDouble($"Longitude_LIST")).
      withColumn("Altitude_LIST", Util.flatMapDouble($"Altitude_LIST")).
      withColumn("Fault_Code_LIST", Util.flatMapStr($"Fault_Code_LIST")).
      withColumn("Fault_Code_Description_LIST", Util.flatMapStr($"Fault_Code_Description_LIST")).
      withColumn("Derate_Flag_LIST", Util.flatMapStr($"Derate_Flag_LIST")).
      withColumn("Shutdown_Flag_LIST", Util.flatMapStr($"Shutdown_Flag_LIST")).
      withColumn("Fault_Code_Category_LIST", Util.flatMapStr($"Fault_Code_Category_LIST")).
      withColumn("nonIm_Occurrence_Date_Time_LIST", Util.flatTimestampMapStrSet($"nonIm_Occurrence_Date_Time_LIST")).
      withColumn("nonIm_Latitude_LIST", Util.flatMapDouble($"nonIm_Latitude_LIST")).
      withColumn("nonIm_Longitude_LIST", Util.flatMapDouble($"nonIm_Longitude_LIST")).
      withColumn("nonIm_Altitude_LIST", Util.flatMapDouble($"nonIm_Altitude_LIST")).
      withColumn("nonIm_Fault_Code_LIST", Util.flatMapStr($"nonIm_Fault_Code_LIST")).
      withColumn("nonIm_Fault_Code_Description_LIST", Util.flatMapStr($"nonIm_Fault_Code_Description_LIST")).
      withColumn("nonIm_Derate_Flag_LIST", Util.flatMapStr($"nonIm_Derate_Flag_LIST")).
      withColumn("nonIm_Shutdown_Flag_LIST", Util.flatMapStr($"nonIm_Shutdown_Flag_LIST")).
      withColumn("nonIm_Fault_Code_Category_LIST", Util.flatMapStr($"nonIm_Fault_Code_Category_LIST")).
      withColumn("REL_CMP_CLAIM_NUM_LIST", Util.flatMapStr($"REL_CMP_CLAIM_NUM_LIST")).
      withColumn("REL_CMP_FAIL_CODE_LIST", Util.flatMapStr($"REL_CMP_FAIL_CODE_LIST")).
      withColumn("REL_CMP_FAILURE_PART_LIST", Util.flatMapStr($"REL_CMP_FAILURE_PART_LIST")).
      withColumn("REL_CMP_ABO_FAILURE_PART_LIST", Util.flatMapStr($"REL_CMP_ABO_FAILURE_PART_LIST")).
      withColumn("REL_CMP_DEALER_NAME_LIST", Util.flatMapStr($"REL_CMP_DEALER_NAME_LIST")).
      withColumn("REL_CMP_CLAIM_DATE_LIST", Util.flatTimestampMapStrSet($"REL_CMP_CLAIM_DATE_LIST")).
      withColumn("REL_CMP_PART_NUM_LIST", Util.flatMapStr($"REL_CMP_PART_NUM_LIST")).
      withColumn("REL_CMP_BASE_OR_ATS_LIST", Util.flatMapStr($"REL_CMP_BASE_OR_ATS_LIST")).
      withColumn("REL_CMP_PAID_OPEN_LIST", Util.flatMapStr($"REL_CMP_PAID_OPEN_LIST")).
      withColumn("REL_CMP_DATA_PROVIDER_LIST", Util.flatMapStr($"REL_CMP_DATA_PROVIDER_LIST")).
      withColumn("REL_CMP_FAIL_DATE_LIST",  Util.flatTimestampMapStrSet($"REL_CMP_FAIL_DATE_LIST")).
      withColumn("REL_CMP_CAUSE_LIST", Util.flatMapStr($"REL_CMP_CAUSE_LIST")).
      withColumn("REL_CMP_COMPLAINT_LIST", Util.flatMapStr($"REL_CMP_COMPLAINT_LIST")).
      withColumn("REL_CMP_CORRECTION_LIST", Util.flatMapStr($"REL_CMP_CORRECTION_LIST")).
      withColumn("REL_CMP_FAILURE_MODE_LIST",Util.flatMapStr($"REL_CMP_FAILURE_MODE_LIST")).
      withColumn("REL_CMP_SUM_NET_AMOUNT", udfDistinctSum("REL_CMP_SUM_NET_AMOUNT")($"REL_CMP_SUM_NET_AMOUNT")).
      withColumn("REL_CMP_SUM_LABOR_HOURS", udfDistinctSum("REL_CMP_SUM_LABOR_HOURS")($"REL_CMP_SUM_LABOR_HOURS")).
      withColumn("Report_Type", when($"Report_Type".isNull, lit("REL")).otherwise($"Report_Type")).
      withColumn("Mileage_LIST",Util.flatMapInt($"Mileage_LIST")).
      withColumn("nonIm_Mileage_LIST",Util.flatMapInt($"nonIm_Mileage_LIST")).
      withColumn("WORK_HOUR_LIST",Util.flatMapInt($"WORK_HOUR_LIST")).
      withColumn("nonIm_WORK_HOUR_LIST",Util.flatMapInt($"nonIm_WORK_HOUR_LIST")).
      withColumn("FAULT_CODE_REASON_LIST",Util.flatMapStr($"FAULT_CODE_REASON_LIST")).
      withColumn("NONIM_FAULT_CODE_REASON_LIST",Util.flatMapStr($"NONIM_FAULT_CODE_REASON_LIST")).
      withColumn("CALIBRATION_VERSION_LIST",Util.flatMapStr($"CALIBRATION_VERSION_LIST")).
      withColumn("nonIm_CALIBRATION_VERSION_LIST",Util.flatMapStr($"nonIm_CALIBRATION_VERSION_LIST"))

    // select(orderedColumns: _*).
    //  withColumn("EARLIEST_INDICATION_DATE", createEarliestIndicationDate(to_date($"DSID_CREATE_DATE"), $"REL_CMP_FAIL_DATE_LIST")).
    //  withColumn("KNOWLEDGE_DATE", createKnowledgeDate($"REL_CMP_PAYMENT_DATE_LIST", $"DSID_CREATE_DATE_LIST"))

    aggregatedIncidentTracker
  }

  // Sum array of doubles from a Row. e.g. [[WrappedArray(123, 10), 105.10], WrappedArray(300), 200.86]]] => 305.96
  def udfDistinctSum(colToSum: String): UserDefinedFunction = udf((xs: Seq[Row]) => xs.map(_.getAs[Double](colToSum)).sum)

  //Below we are trying to calculate the earliest indication date of any incidents.
  // We do not have it data thus we genereate it based on the logic provided by translators.
  val createEarliestIndicationDate = udf(
    (dsidCreateDate: Date, claimDateList: WrappedArray[String]) => {
      if (dsidCreateDate != null) {
        dsidCreateDate.toString
      } else if (claimDateList.nonEmpty) {
        claimDateList.min.toString
      } else {
        null
      }
    }
  )

  val createKnowledgeDate = udf(
    (RelDateList: WrappedArray[String], EdsDateList: WrappedArray[String]) => {
      if (RelDateList.nonEmpty) RelDateList.min.toString
      else if (EdsDateList.nonEmpty) EdsDateList.min.toString
      else null
    }
  )
}


