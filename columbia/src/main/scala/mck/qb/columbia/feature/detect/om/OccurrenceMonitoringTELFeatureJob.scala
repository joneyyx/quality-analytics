package mck.qb.columbia.feature.detect.om

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.Job
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Prepare Occurrence Monitoring EDS data. Feature layer job, all data is saved to the correct schema/db.
  */
object OccurrenceMonitoringTELFeatureJob extends Job with Transport {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.FET_DETECT_OM_TEL)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  def getIncTblNames(): Seq[String] =

  Seq(Constants.ALL_INC_F_TBL, Constants.ALL_INC_X_TBL, Constants.ALL_INC_B_TBL, Constants.ALL_INC_D_TBL, Constants.ALL_INC_L_TBL, Constants.ALL_INC_Z_TBL, Constants.ALL_INC_M_TBL,Constants.ALL_INC_NG_TBL)

  override def run() = {


    //loading all input incidents data sets for each supported engine platform, and union them together
    val incidents = getIncTblNames().map((tbl: String) => loadIncidents(s"${cfg(Constants.FET_DB)}.${cfg(tbl)}")).reduce(_ union _).persist(StorageLevel.MEMORY_AND_DISK)

    val engineBuilds = loadEngFeatures(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}")
      .withColumn("REL_ENGINE_NAME_DESC",concat_ws("_",$"REL_ENGINE_NAME_DESC",$"REL_ENGINE_PLANT"))
      .withColumnRenamed("ENGINE_DETAILS_ESN", "REL_ESN")

    val incidentsByEngineBuilds = incidents.
      join(engineBuilds, "REL_ESN").drop(engineBuilds("REL_ESN")).
      distinct().cache()

    val distinctESNsWithBuildDate = incidentsByEngineBuilds.
      select("REL_ESN", "REL_BUILD_DATE").
      distinct()

    distinctESNsWithBuildDate.createOrReplaceTempView("distinctDFV")

    val explodeDFSQL =
      """
        |SELECT
        |REL_ESN,
        |REL_BUILD_DATE,
        |'NA' AS CODE,
        |DATE_ADD(T.REL_BUILD_DATE, PE.I) AS D_DATE,
        |'TEL' AS SOURCE
        |FROM
        |DISTINCTDFV T
        |LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(CAST(CURRENT_TIMESTAMP() AS DATE), REL_BUILD_DATE)), ' ')) PE AS I, X""".stripMargin

    val explodeDF = load(explodeDFSQL).distinct()

    val explodeFaultDF1 = incidentsByEngineBuilds.
      select(
        $"REL_ESN",
        $"REL_BUILD_DATE",
        to_date($"TEL_FAILURE_DATE").alias("D_DATE"),
        explode($"Fault_Code_LIST").alias("Exploded_Fault_Code")).
      filter("TEL_FAILURE_DATE is not null").
      distinct()

    val joinedDF1 = explodeDF.
      join(explodeFaultDF1, Seq("REL_ESN", "REL_BUILD_DATE", "D_DATE"), "left_outer").
      withColumn("CODE", coalesce($"Exploded_Fault_Code", $"CODE")).withColumn("reporttype", lit("Immediate")).
      drop("Exploded_Fault_Code")

    val explodeFaultDF2 = incidentsByEngineBuilds.
      select(
        $"REL_ESN",
        $"REL_BUILD_DATE",
        to_date($"TEL_FAILURE_DATE").alias("D_DATE"),
        explode($"nonIm_Fault_Code_LIST").alias("Exploded_Fault_Code")).
      filter("TEL_FAILURE_DATE is not null").
      distinct()

    val joinedDF2 = explodeDF.
      join(explodeFaultDF2, Seq("REL_ESN", "REL_BUILD_DATE", "D_DATE"), "left_outer").
      withColumn("CODE", coalesce($"Exploded_Fault_Code", $"CODE")).withColumn("reporttype", lit("NonImmediate")).
      drop("Exploded_Fault_Code")


    val distinctEdsBuildDateCode = joinedDF1.union(joinedDF2).select($"REL_ESN", $"REL_BUILD_DATE", $"D_DATE", $"CODE").distinct()

    val codeCounts = distinctEdsBuildDateCode.
      groupBy($"REL_ESN", $"REL_BUILD_DATE", date_sub(next_day($"D_DATE", "Wed"), 1).alias("CALC_DATE"), $"CODE").
      agg(count($"CODE").alias("COUNT"))

    val addtnlDimBeforePlatformAndMilage = incidentsByEngineBuilds.
      select($"REL_ESN",
        concat(year($"REL_BUILD_DATE"), lit("-Q"), quarter($"REL_BUILD_DATE")).alias("REL_QUARTER_BUILD_DATE"),
        concat(year($"REL_BUILD_DATE"), lpad(month($"REL_BUILD_DATE"), 2, "0")).alias("REL_MONTH_BUILD_DATE"),
        year($"REL_BUILD_DATE").alias("REL_YEAR_BUILD_DATE"),
        $"REL_OEM_NAME",
        $"REL_ENGINE_PLANT",
        $"REL_USER_APPL_DESC",
        $"REL_ENGINE_NAME_DESC",
        $"REL_CMP_ENGINE_NAME",
        $"REL_CMP_COVERAGE",
        $"REL_OEM_NORMALIZED_GROUP"
      ).distinct()


    val addtnlDim = addtnlDimBeforePlatformAndMilage.
      withColumn("REL_MILAGE", lit(0)).
      withColumn("SOURCE", lit("TEL"))

    val telOMData = codeCounts.join(addtnlDim, Seq("REL_ESN"), "left_outer").
      withColumn("REF_FTE_CMP_IS_TEST_ENGINE", lit(0)).persist(StorageLevel.MEMORY_AND_DISK)


    val fieldAndNonField: (Dataset[Row], Dataset[Row]) = getFieldAndNonField(telOMData)

    save(fieldAndNonField._1, cfg(Constants.FET_DB),cfg(Constants.OCM_TEL_MTR_FLD_TBL))
    save(fieldAndNonField._2, cfg(Constants.FET_DB), cfg(Constants.OCM_TEL_MTR_NON_FLD_TBL))


//    println("INFO::SAVING DATA IN TABLE:" + cfg(Constants.FET_DB) + "." + cfg(Constants.OCM_TEL_MTR_TBL))
//
//    save(telOMData, cfg(Constants.FET_DB), cfg(Constants.OCM_TEL_MTR_TBL))

  }

  private def getFieldAndNonField(telOMData: DataFrame) = {
    val ref_field_test_esn: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_TEL_FT_ESN_TBL)}")
      .select("ESN")
      .withColumnRenamed("ESN","REL_ESN")
      .withColumn("flag", lit(1))
    val joined_df: DataFrame = telOMData
      .join(ref_field_test_esn, Seq("REL_ESN"), "left_outer")
      .drop("REL_CMP_COVERAGE")

    val nan_field: Dataset[Row] = joined_df
      .where($"flag" isNull)
      .drop("flag")
      .withColumn("SOURCE",lit("TEL-No FT"))
    val field: Dataset[Row] = joined_df
      .where($"flag" isNotNull)
      .drop("flag")
      .withColumn("SOURCE",lit("TEL-FT"))
    (field,nan_field)
  }

  /**
    * Loads default set of columns from engine features table
    *
    * @param engFeatTblName [engine-platform-specific] engine features table (from Reliability)
    * @return data
    */
  def loadEngFeatures(engFeatTblName: String) = {
    load(engFeatTblName,
      "ENGINE_DETAILS_ESN",
      "REL_SO_NUM",
      "REL_BUILD_DATE",
      "REL_EMISSION_LEVEL",
      "REL_ATS_TYPE",
      "REL_HORSEPOWER",
      "REL_USER_APPL_DESC",
      "REL_VEHICLE_TYPE",
      "REL_OEM_NAME",
      "REL_ENGINE_PLANT",
      "REL_IN_SERVICE_DATE_AVE_TIME_LAG",
      "REL_ENGINE_TYPE_DESC",
      "REL_WAREHOUSE",
      "REL_ACTUAL_IN_SERVICE_DATE",
      "REL_ENGINE_PLATFORM",
      "REL_ENGINE_GROUP_DESC",
      "REL_ABO_QUALITY_PERFORMANCE_GROUP",
      "REL_CCI_CHANNEL_SALES_Y_N",
      "REL_BUILD_YEAR",
      "REL_BUILD_MONTH",
      "REL_BUILD_DAY_OF_MONTH",
      "REL_BUILD_DAY_OF_WEEK",
      "REL_BUILT_ON_WEEKEND",
      "REL_Normal_EMISSION_LEVEL",
      "REL_ENGINE_NAME_DESC",
      "REL_CMP_TIME_DIFF_BUILDDATE_TO_INSERVICEDATE_DAYS",
      "REL_CMP_ENGINE_NAME",
      "REL_OEM_NORMALIZED_GROUP"
    )
  }

  /**
    * Loads default set of columns from Incidents table
    *
    * @param incTblName [engine-platform-specific] incidents table
    * @return data
    */
  def loadIncidents(incTblName: String) = {
    load(incTblName,
      "GROUP_BY_ID",
      "GROUP_BY_DATE",
      "REL_ESN",
      "REL_FAILURE_DATE",
      "TEL_FAILURE_DATE",
      "Report_Type",
      "REL_CMP_COVERAGE",
      "REL_CMP_IN_SERVICE_DATE",
      "Occurrence_Date_Time_LIST",
      "Latitude_LIST",
      "Longitude_LIST",
      "Altitude_LIST",
      "Fault_Code_LIST",
      "Fault_Code_Description_LIST",
      "Derate_Flag_LIST",
      "Shutdown_Flag_LIST",
      "Fault_Code_Category_LIST",
      "nonIm_Occurrence_Date_Time_LIST",
      "nonIm_Latitude_LIST",
      "nonIm_Longitude_LIST",
      "nonIm_Altitude_LIST",
      "nonIm_Fault_Code_LIST",
      "nonIm_Fault_Code_Description_LIST",
      "nonIm_Derate_Flag_LIST",
      "nonIm_Shutdown_Flag_LIST",
      "nonIm_Fault_Code_Category_LIST",
      "REL_CMP_ENGINE_MILES_LIST",
      "REL_CMP_CLAIM_NUM_LIST",
      "REL_CMP_FAIL_CODE_LIST",
      "REL_CMP_FAILURE_PART_LIST",
      "REL_CMP_DEALER_NAME_LIST",
      "REL_CMP_CLAIM_DATE_LIST",
      "REL_CMP_PART_NUM_LIST",
      "REL_CMP_BASE_OR_ATS_LIST",
      "REL_CMP_PAID_OPEN_LIST",
      "REL_CMP_DATA_PROVIDER_LIST",
      "REL_CMP_SUM_NET_AMOUNT",
      "REL_CMP_SUM_LABOR_HOURS"
    )
  }
}

