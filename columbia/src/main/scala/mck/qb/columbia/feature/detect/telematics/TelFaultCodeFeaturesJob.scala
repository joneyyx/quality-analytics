package mck.qb.columbia.feature.detect.telematics

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

object TelFaultCodeFeaturesJob extends Job {

  import spark.implicits._

  var myConf: Config = AppConfig.getMyConfig(Constants.FET_TEL_FC_DTL)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString


  override def run(): Unit = {

    //QA_DEV_PRIMARY.TEL_FAULT_CODE_DETAIL
    val telfcDataRaw: Dataset[Row] = getTelfcNotFilteredDF.persist(StorageLevel.MEMORY_AND_DISK)
    val referencereasonDF: DataFrame = getReferenceReason(telfcDataRaw)
    // Do we want to distinguish which phase the eds fauld codes went to?

    // (ImmediateFaultCodes, nonImmediateFaultCodes)
    val processDfTuple: (DataFrame, DataFrame) = makeProcessDfTuple(referencereasonDF)
    val ImmediateFaultCodes: DataFrame = processDfTuple._1
    val nonImmediateFaultCodes: DataFrame = processDfTuple._2

    // quality_compute.EDS_FINAL_FEATURES
    val joinCondition: Column = ImmediateFaultCodes("I_ESN") === nonImmediateFaultCodes("N_ESN") && ImmediateFaultCodes("I_FAILURE_DATE") === nonImmediateFaultCodes("FAILURE_DATE")
    val final_Tel: DataFrame = ImmediateFaultCodes.join(nonImmediateFaultCodes, joinCondition, "outer")//testing
      .withColumn("Report_Type",
      when(col("Immediate").isNull, lit("TELFC_NonImmediate_ONLY"))
        .otherwise(when(col("NonImmediate").isNull, lit("TELFC_Immediate_ONLY"))
          .otherwise(lit("TELFC_Both"))))
      .withColumn("ESN",coalesce($"I_ESN",$"N_ESN"))
      .withColumn("FAILURE_DATE",coalesce($"I_FAILURE_DATE",$"FAILURE_DATE"))
      .drop("I_ESN","I_FAILURE_DATE","N_ESN")
      .persist(StorageLevel.MEMORY_AND_DISK)

    //Save to Hive QA_DEV_FEATURE.TEL_FINAL_FEATURES
    Util.saveData(final_Tel, cfg(Constants.FET_DB), cfg(Constants.TEL_FNL_FET_TBL), cfg(Constants.HDFS_PATH))
    //if we need to save for DS
    //Util.saveData(final_Tel, cfg(Constants.FET_DB), cfg(Constants.TEL_FNL_FET_TBL), cfg(Constants.HDFS_PATH),"overwrite","csv")

  }

  private def makeProcessDfTuple(telfcDataRaw: Dataset[Row]): (DataFrame, DataFrame) = {
    //Change Latitude/Longitude is null to Latitude/Longitude=0 before agg()
    val telfcDataRaw2 = telfcDataRaw.
      withColumn("Latitude",when($"Latitude".isNotNull,$"Latitude").otherwise(lit(0))).
      withColumn("Longitude",when($"Longitude".isNotNull,$"Longitude").otherwise(lit(0))).
      withColumn("Altitude",when($"Altitude".isNotNull,$"Altitude").otherwise(lit(0)))

    val cv_nsvi_esn: DataFrame = spark.table(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}")
      .select("engine_details_esn")
      .where(upper($"rel_normal_emission_level") === "NSVI")
      .distinct()
      .coalesce(1)

    val telfc_nsv_nsvi: DataFrame = telfcDataRaw2
      .join(cv_nsvi_esn, telfcDataRaw2("Engine_Serial_Number") === cv_nsvi_esn("engine_details_esn"),"left")

    val nsv_telfc_df: DataFrame = telfc_nsv_nsvi
      .filter($"engine_details_esn".isNull)
      .drop("engine_details_esn")

    val nsvi_telfc_df: DataFrame = telfc_nsv_nsvi
      .filter($"engine_details_esn".isNotNull)
      .drop("CALIBRATION_VERSION","engine_details_esn")

    val trim_data: DataFrame = spark.table(s"${cfg(Constants.TRIM_DB)}.${cfg(Constants.TEL_CSU_TRIM_TBL)}")
      .select("REPORT_DATE","ESN","ECM_CODE")
      .coalesce(1)

    val nsvi_with_cv: DataFrame = nsvi_telfc_df
      .join(trim_data,
        nsvi_telfc_df("Occurrence_Date_Time")===trim_data("REPORT_DATE") && nsvi_telfc_df("Engine_Serial_Number")===trim_data("ESN"),
        "left")
      .withColumnRenamed("ECM_CODE","CALIBRATION_VERSION")
      .drop("REPORT_DATE")
      .drop(trim_data("ESN"))

    val telfcDataRaw3: Dataset[Row] = nsv_telfc_df.union(nsvi_with_cv).
      withColumn("CALIBRATION_VERSION",when($"CALIBRATION_VERSION".rlike("^[a-zA-Z0-9]+\\.?+[a-zA-Z0-9]+$"),$"CALIBRATION_VERSION").otherwise(null)).
      cache()

    val ImmediateFaultCodes: DataFrame = telfcDataRaw3.filter($"Report_Type" === "Immediate" || $"Report_Type" === "Immediate - Seek Service" || $"Report_Type" === "Immediate - Contact Driver").
      withColumn("Occur_Date", to_date($"Occurrence_Date_Time")).
      groupBy($"Engine_Serial_Number",$"Occur_Date")
      .agg(first("Engine_Serial_Number").as("I_ESN"),
        min("Occur_Date").as("I_FAILURE_DATE"),
        collect_list("Occurrence_Date_Time").as("Occurrence_Date_Time_LIST"),
        collect_list("Latitude").as("Latitude_LIST"),
        collect_list("Longitude").as("Longitude_LIST"),
        collect_list("Altitude").as("Altitude_LIST"),
        collect_list("Fault_Code").as("Fault_Code_LIST"),
        collect_list("Fault_Code_Description").as("Fault_Code_Description_LIST"),
        collect_list("Derate_Flag").as("Derate_Flag_LIST"),
        collect_list("Shutdown_Flag").as("Shutdown_Flag_LIST"),
        collect_list("Fault_Code_Category").as("Fault_Code_Category_LIST"),
        collect_list("Mileage").as("Mileage_LIST"),
        collect_list("WORK_HOUR").as("WORK_HOUR_LIST"),
        collect_set("REASON").as("FAULT_CODE_REASON_LIST"),
        collect_set("CALIBRATION_VERSION").as("CALIBRATION_VERSION_LIST")
      ).
      //	  withColumn("Report_Type",lit(Immediate))
      withColumn("Immediate", lit("Immediate")).
      drop("Occur_Date")

    val nonImmediateFaultCodes: DataFrame = telfcDataRaw3.filter(!($"Report_Type" === "Immediate" || $"Report_Type" === "Immediate - Seek Service" || $"Report_Type" === "Immediate - Contact Driver")).
      withColumn("Occur_Date", to_date($"Occurrence_Date_Time")).
      groupBy($"Engine_Serial_Number", $"Occur_Date")
      .agg(
        first("Engine_Serial_Number").as("N_ESN"),
        min("Occur_Date").as("FAILURE_DATE"),
        collect_list("Occurrence_Date_Time") as "nonIm_Occurrence_Date_Time_LIST",
        collect_list("Latitude") as "nonIm_Latitude_LIST",
        collect_list("Longitude") as "nonIm_Longitude_LIST",
        collect_list("Altitude") as "nonIm_Altitude_LIST",
        collect_list("Fault_Code") as "nonIm_Fault_Code_LIST",
        collect_list("Fault_Code_Description") as "nonIm_Fault_Code_Description_LIST",
        collect_list("Derate_Flag") as "nonIm_Derate_Flag_LIST",
        collect_list("Shutdown_Flag") as "nonIm_Shutdown_Flag_LIST",
        collect_list("Fault_Code_Category") as "nonIm_Fault_Code_Category_LIST",
        collect_list("Mileage") as "nonIm_Mileage_LIST",
        collect_list("WORK_HOUR") as "nonIm_WORK_HOUR_LIST",
        collect_set("REASON") as "NONIM_FAULT_CODE_REASON_LIST",
        collect_set("CALIBRATION_VERSION") as "nonIm_CALIBRATION_VERSION_LIST"
      ).
      withColumn("NonImmediate", lit("NonImmediate"))
      .drop("Engine_Serial_Number")
    (ImmediateFaultCodes, nonImmediateFaultCodes)
  }

  def getTelfcNotFilteredDF: Dataset[Row] = {
    val strSql: String =
      s"""
         |SELECT TELEMATICS_PARTNER_NAME,
         |OCCURRENCE_DATE_TIME,
         |LATITUDE,
         |LONGITUDE,
         |ALTITUDE,
         |ENGINE_SERIAL_NUMBER,
         |SERVICE_MODEL_NAME,
         |ACTIVE,
         |FAULT_CODE,
         |FAULT_CODE_DESCRIPTION,
         |DERATE_FLAG,
         |LAMP_COLOR,
         |REPORT_TYPE,
         |SHUTDOWN_FLAG,
         |PRIORITY,
         |FAULT_CODE_CATEGORY,
         |Mileage/1000 as Mileage,
         |WORK_HOUR,
         |CALIBRATION_IDENTIFICATION as CALIBRATION_VERSION
         | FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.TEL_FC_DTL_TBL)}
      """.stripMargin

    val strjoin: String =
      s"""
         |SELECT ENGINE_DETAILS_ESN,
         |REL_ENGINE_NAME_DESC
         | FROM ${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}
      """.stripMargin

    val rel_engine_features: DataFrame = spark.sql(strjoin)
    val telfcDataRaw: Dataset[Row] = spark.sql(strSql)

    val hb_agg_infoSql: String =
      s"""
         |SELECT ESN,
         |DATE,
         |DISTANCE
         | FROM ${cfg(Constants.REF_DB)}.${cfg(Constants.REF_HB_AGG_INFO_TBL)}
       """.stripMargin
    val hb_agg_info: DataFrame = spark.sql(hb_agg_infoSql)

    val telfc_join_hbagg: DataFrame = telfcDataRaw
      .join(hb_agg_info,
        telfcDataRaw("ENGINE_SERIAL_NUMBER") === hb_agg_info("ESN")
          && telfcDataRaw("OCCURRENCE_DATE_TIME") >= hb_agg_info("DATE"),"left")
      .groupBy("TELEMATICS_PARTNER_NAME",
        "OCCURRENCE_DATE_TIME",
        "LATITUDE",
        "LONGITUDE",
        "ALTITUDE",
        "ENGINE_SERIAL_NUMBER",
        "SERVICE_MODEL_NAME",
        "ACTIVE",
        "FAULT_CODE",
        "FAULT_CODE_DESCRIPTION",
        "DERATE_FLAG",
        "LAMP_COLOR",
        "REPORT_TYPE",
        "SHUTDOWN_FLAG",
        "PRIORITY",
        "FAULT_CODE_CATEGORY",
        "CALIBRATION_VERSION")
      .agg(
        sum("DISTANCE").alias("Mileage"),
        sum("WORK_HOUR").alias("WORK_HOUR")
      )

    val telfc_join_releng: DataFrame = telfc_join_hbagg.join(
      rel_engine_features,
      telfcDataRaw("ENGINE_SERIAL_NUMBER") === rel_engine_features("ENGINE_DETAILS_ESN")
    ).select(
      "TELEMATICS_PARTNER_NAME",
      "OCCURRENCE_DATE_TIME",
      "LATITUDE",
      "LONGITUDE",
      "ALTITUDE",
      "ENGINE_SERIAL_NUMBER",
      "SERVICE_MODEL_NAME",
      "ACTIVE",
      "FAULT_CODE",
      "FAULT_CODE_DESCRIPTION",
      "DERATE_FLAG",
      "LAMP_COLOR",
      "REPORT_TYPE",
      "SHUTDOWN_FLAG",
      "PRIORITY",
      "FAULT_CODE_CATEGORY",
      "Mileage",
      "WORK_HOUR",
      "REL_ENGINE_NAME_DESC",
      "CALIBRATION_VERSION"
    )

    val strFilt: String =
      s"""
         |ENGINE_SERIAL_NUMBER IN (
         |SELECT ENGINE_DETAILS_ESN
         |FROM ${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)})
      """.stripMargin

    val telfcData: Dataset[Row] = telfc_join_releng
      .filter($"Active" === "1")
      .where(strFilt)

    telfcData
  }

  def getReferenceReason(df:DataFrame):DataFrame = {
    val ref_telfc_desc: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_TFC_DESC_TBL)}")
        .withColumnRenamed("rel_engine_name_desc","rel_engine_name_desc_new")

    df.join(ref_telfc_desc,
      df("FAULT_CODE")===ref_telfc_desc("code")&&
      df("REL_ENGINE_NAME_DESC")===ref_telfc_desc("rel_engine_name_desc_new"),
      "left_outer"
    ).select(
      "TELEMATICS_PARTNER_NAME",
      "OCCURRENCE_DATE_TIME",
      "LATITUDE",
      "LONGITUDE",
      "ALTITUDE",
      "ENGINE_SERIAL_NUMBER",
      "SERVICE_MODEL_NAME",
      "ACTIVE",
      "FAULT_CODE",
      "FAULT_CODE_DESCRIPTION",
      "DERATE_FLAG",
      "LAMP_COLOR",
      "REPORT_TYPE",
      "SHUTDOWN_FLAG",
      "PRIORITY",
      "FAULT_CODE_CATEGORY",
      "Mileage",
      "WORK_HOUR",
      "REL_ENGINE_NAME_DESC",
      "REASON",
      "CALIBRATION_VERSION"
    )
  }
}
