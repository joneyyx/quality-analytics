package mck.qb.columbia.modelInput.weibull

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

/**
  * Created by newforesee on 2020/3/4
  */
object WeibullModelInputJob extends Job with Transport {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_WB_FET)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString


  case class Writer(df: DataFrame, tbl: String)

  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {

    val engineFetFilted = loadRelEngineFetFiltered().persist(StorageLevel.MEMORY_AND_DISK)
    //do some common process
    val common_df: DataFrame = getCommonEngineFeatures(engineFetFilted)

    Seq(
      Writer(getWeibullEngineFeatures(common_df), cfg(Constants.MIP_WB_ENG_TBL)),
      Writer(getWeibullClaimFeatures(common_df), cfg(Constants.MIP_WB_CLM_TBL))
    ).foreach(writer => {
      logger.warn("INFO :: Saving >>>>>> [%s]".format(writer.tbl))
      writer match {
        case Writer(df, tbl) => Util.saveData(df, cfg(Constants.MODEL_IP_DB), tbl, cfg(Constants.HDFS_PATH))
      }
    })

  }

  /**
    *
    * @return
    */
  private def loadRelEngineFetFiltered() = {
    load(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}",
      "REL_CMP_ENGINE_NAME",
      "REL_BUILD_DATE",
      "REL_USER_APPL_DESC",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_ENGINE_NAME_DESC",
      "REL_ENGINE_PLANT",
      "ENGINE_DETAILS_ESN",
      "REL_BUILD_YEAR",
      "REL_ENGINE_PLATFORM",
      "REL_ACTUAL_IN_SERVICE_DATE",
      "REL_NORMAL_EMISSION_LEVEL"
    )
      .join(load(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_CLM_FET_TBL)}", "REL_ESN", "REL_CMP_COVERAGE"),
        $"ENGINE_DETAILS_ESN" === $"REL_ESN",
        "left")
      .drop("REL_ESN")
      .where("REL_NORMAL_EMISSION_LEVEL = 'NSVI' and REL_CMP_COVERAGE in ('Emission','Warranty')")
  }

  /**
    * Co-processing of EngineFeatures
    *
    * @param engineFetFilted
    * @return
    */
  private def getCommonEngineFeatures(engineFetFilted: Dataset[Row]) = {
    val common_df = engineFetFilted
      .withColumn("REL_BUILD_QUARTER", concat(year($"REL_BUILD_DATE"), lit("-Q") , quarter($"REL_BUILD_DATE")))
      .withColumn("REL_ENGINE_NAME_DESC", concat($"REL_ENGINE_NAME_DESC", lit("_"), $"REL_ENGINE_PLANT"))
      .withColumn("USER_APPL_CODE", Util.getUserAppCode($"REL_ENGINE_PLATFORM", $"REL_USER_APPL_DESC"))
      .withColumnRenamed("REL_ACTUAL_IN_SERVICE_DATE", "REL_IN_SERVICE_DATE")
      .withColumn("USER_APPL_TYPE", Util.getUserAppType($"REL_ENGINE_PLATFORM", $"REL_USER_APPL_DESC"))
      .withColumn("SOURCE", lit("REL"))
    common_df
  }


  /**
    * Select the columns needed for WEIBULL_ENGINE_FEATURES
    *
    * @param common_df
    * @return
    */
  private def getWeibullEngineFeatures(common_df: DataFrame) = {
    common_df.select("REL_CMP_ENGINE_NAME",
      "REL_ENGINE_PLATFORM",
      "REL_BUILD_QUARTER",
      "REL_USER_APPL_DESC",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_ENGINE_NAME_DESC",
      "ENGINE_DETAILS_ESN",
      "REL_BUILD_DATE",
      "REL_BUILD_YEAR",
      "USER_APPL_CODE",
      "REL_IN_SERVICE_DATE"
    )
  }

  /**
    * Build ClaimFeatures using reference table and REL_ENGINE_CLAIM_DETAIL
    *
    * @param common_df
    * @return
    */
  private def getWeibullClaimFeatures(common_df: DataFrame) = {
    //加载构建reference表
    val ref_prt_type = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_PRT_TYPE_WRT_TBL)}")

    val reference = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_PRT_TYPE_MP_TBL)}")
      .join(
        ref_prt_type,
        Seq("WARRANTY_PART"),
        "outer")
      .withColumn("WARRANTY_PERIOD_TIME", regexp_replace($"WARRANTY_PERIOD_TIME", "M", ""))
      .withColumn("WARRANTY_PERIOD_MILEAGE", regexp_replace($"WARRANTY_PERIOD_MILEAGE", "KM", ""))
      .withColumn("FAILURE_PART", when($"WARRANTY_PART".isin("ATS", "Sensor/传感器"), $"WARRANTY_PART").otherwise($"FAILURE_PART"))

    //加载REL_ENGINE_CLAIM_DETAIL并在列名前加"REL_",单独处理BASE_OR_ATS="ATS"的传感器零件
    val rel_engine_claim_dtl =
      Util.formatCapitalizeNamesWithPrefix(
        load(s"${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_CLM_DTL_TBL)}",
          "ESN_NEW",
          "COVERAGE",
          "MILEAGE",
          "TOTAL_COST",
          "FAILURE_DATE",
          "CLAIM_SUBMIT_DATE",
          "EW_PART_OR_NOT_NS_VI_ONLY",
          "FAILURE_PART",
          "BASE_OR_ATS"
        ), "REL_")
        .withColumn("REL_CMP_FAIL_CODE", $"REL_FAILURE_PART")
        .withColumnRenamed("REL_TOTAL_COST", "REL_CMP_SUM_NET_AMOUNT")
        .withColumnRenamed("REL_CLAIM_SUBMIT_DATE", "REL_CMP_CLAIM_DATE")
        .withColumnRenamed("REL_MILEAGE", "REL_CMP_ENGINE_MILES")
        .withColumn("PART_WARRANTY_TYPE",
          when($"REL_EW_PART_OR_NOT_NS_VI_ONLY" === "Y", lit("EMISSION"))
            .otherwise(lit("NON_EMISSION")))
        .withColumn("REL_FAILURE_PART",
          when(Util.toUpperCase($"REL_FAILURE_PART").rlike("%SENSOR%") and $"REL_BASE_OR_ATS" === "ATS", lit("Sensor/传感器"))
            .otherwise(when($"REL_BASE_OR_ATS" === "ATS" and $"REL_FAILURE_PART".rlike("%传感器%"), lit("Sensor/传感器"))
              .otherwise(when($"REL_BASE_OR_ATS" === "ATS", lit("ATS"))
                .otherwise($"REL_FAILURE_PART")))
        )
        .withColumnRenamed("REL_FAILURE_PART", "FAILURE_PART")
    //
    val ref_tmp = reference.select("USER_APPL_TYPE", "WARRANTY_PART", "FAILURE_PART")
    common_df.join(rel_engine_claim_dtl, $"ENGINE_DETAILS_ESN" === $"REL_ESN_NEW", "left")
      .join(ref_tmp,Seq("FAILURE_PART", "USER_APPL_TYPE"),"left")
      .withColumn("WARRANTY_PART",when($"WARRANTY_PART" isNull ,lit("OTHER")).otherwise($"WARRANTY_PART"))
      .join(reference, Seq("WARRANTY_PART", "USER_APPL_TYPE"), "left")
      .select("REL_ENGINE_NAME_DESC",
        "REL_BUILD_QUARTER",
        "REL_USER_APPL_DESC",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_CMP_ENGINE_NAME",
        "REL_ENGINE_PLATFORM",
        "ENGINE_DETAILS_ESN",
        "REL_BUILD_DATE",
        "REL_BUILD_YEAR",
        "REL_CMP_FAIL_CODE",
        "REL_CMP_COVERAGE",
        "REL_CMP_ENGINE_MILES",
        "REL_CMP_SUM_NET_AMOUNT",
        "REL_FAILURE_DATE",
        "REL_CMP_CLAIM_DATE",
        "PART_WARRANTY_TYPE",
        "USER_APPL_CODE",
        "SOURCE",
        "PART_TYPE",
        "WARRANTY_PERIOD_TIME",
        "WARRANTY_PERIOD_MILEAGE",
        "OM_LOGIC")
  }


}
