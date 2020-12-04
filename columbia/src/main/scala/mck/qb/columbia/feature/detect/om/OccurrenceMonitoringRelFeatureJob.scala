package mck.qb.columbia.feature.detect.om

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

/**
  * Prepare Occurrence Monitoring Reliability data. Feature layer job, all data is saved to the correct schema/db.
  */
object OccurrenceMonitoringRelFeatureJob extends Job with Transport {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.FET_DETECT_OM_REL)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString
  def getIncTblNames(): Seq[String] = Seq(Constants.ALL_INC_F_TBL,
                                          Constants.ALL_INC_X_TBL,
                                          Constants.ALL_INC_B_TBL,
                                          Constants.ALL_INC_D_TBL,
                                          Constants.ALL_INC_L_TBL,
                                          Constants.ALL_INC_Z_TBL,
                                          Constants.ALL_INC_NG_TBL,
                                          Constants.ALL_INC_M_TBL)

  override def run() = {

    //loading all input incidents data sets for each supported engine platform, and union them together
    val incidents: DataFrame = getIncTblNames()
      .map(tbl => loadIncidents(s"${cfg(Constants.FET_DB)}.${cfg(tbl)}"))
      .reduce(_ union _)
      .cache()

    // define a map of:
    //   target output table name -> input dataframe filtered by program group
    val tblDfMap = Map(
      Constants.OCM_REL_MTR_WARRANTY_TBL -> incidents.filter("REL_CMP_COVERAGE == 'Warranty'"),
      Constants.OCM_REL_MTR_POLICY_TBL -> incidents.filter("REL_CMP_COVERAGE == 'Policy'"),
      Constants.OCM_REL_MTR_CAMPAIGN_TRP_TBL -> incidents.filter("REL_CMP_COVERAGE == 'Campaign_TRP'"),
      Constants.OCM_REL_MTR_FIELD_TEST_TBL -> incidents.filter("REL_CMP_COVERAGE == 'Field_test'"),
      Constants.OCM_REL_MTR_EMISSION_TBL -> incidents.filter("REL_CMP_COVERAGE == 'Emission'"),
      Constants.OCM_REL_MTR_ALL_TBL -> incidents.filter("REL_CMP_COVERAGE in ('Warranty','Policy','Campaign_TRP','Field_test','Emission')")
    )

    // iterate over the map and build output tables
    for ((tbl, df) <- tblDfMap) {
      //println("INFO: Filter and Output Name = ", tbl + " Count:" + df.count)
      createOutputForProgramGroup(df, tbl)
    }
  }

  private def createOutputForProgramGroup(incidents: DataFrame, targetTblNameKey: String) = {

    val allEnginesFeaturesDF = loadEngFeatures(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}")
      .withColumn("REL_ENGINE_NAME_DESC",concat_ws("_",$"REL_ENGINE_NAME_DESC",$"REL_ENGINE_PLANT"))
      .withColumnRenamed("ENGINE_DETAILS_ESN", "REL_ESN").
      cache()

    val incidentsWithEngineFeaturesDF = incidents.
      join(allEnginesFeaturesDF, "REL_ESN").drop(allEnginesFeaturesDF("REL_ESN")).
      distinct()
    incidentsWithEngineFeaturesDF.createOrReplaceTempView("incidentsWithEngineFeaturesDF")

    val explodeRelSQL =
      s"""SELECT
        |REL_ESN,
        |REL_BUILD_DATE,
        |TO_DATE(X) AS D_DATE,
        |X1 AS REL_CMP_FAILURE_PART
        |FROM INCIDENTSWITHENGINEFEATURESDF
        |LATERAL VIEW POSEXPLODE(REL_CMP_FAIL_DATE_LIST) PE AS I, X
        |LATERAL VIEW POSEXPLODE(REL_CMP_FAILURE_PART_LIST) PE1 AS I1, X1
        |WHERE I = I1""".stripMargin
    val explodeRel = load(explodeRelSQL)

    val distinctEsnBuildDates = allEnginesFeaturesDF.
      select("REL_ESN", "REL_BUILD_DATE").
      distinct()
    distinctEsnBuildDates.createOrReplaceTempView("distinctEsnBuildDates")

    val explodeDFSQL =
      s"""
         |SELECT
         |REL_ESN,
         |REL_BUILD_DATE,
         |'NA' AS CODE,
         |DATE_ADD(T.REL_BUILD_DATE,PE.I) AS D_DATE,
         |'REL' AS SOURCE
         |FROM DISTINCTESNBUILDDATES T
         |LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF(CAST(CURRENT_TIMESTAMP() AS DATE),REL_BUILD_DATE)),' ')) PE AS I, X""".stripMargin
    val explodeDF = load(explodeDFSQL).distinct()

    val tempRel = explodeDF.
      join(explodeRel, Seq("REL_ESN", "REL_BUILD_DATE", "D_DATE"), "left_outer").
      withColumn("CODE", coalesce($"REL_CMP_FAILURE_PART", $"CODE")).
      drop("REL_CMP_FAILURE_PART")

    val distRel = tempRel.select($"REL_ESN", $"REL_BUILD_DATE", $"D_DATE", $"CODE").distinct()

    val groupedRel = distRel.
      groupBy(
        $"REL_ESN",
        $"REL_BUILD_DATE",
        date_sub(next_day($"D_DATE", "Wed"), 1).alias("CALC_DATE"),
        $"CODE").
      agg(
        count($"CODE").alias("COUNT")
      )


    val addtnlDimBeforeMilageAndSource = incidentsWithEngineFeaturesDF.select(
      $"REL_ESN",
      concat(year($"REL_BUILD_DATE"), lit("-Q"), quarter($"REL_BUILD_DATE")).alias("REL_QUARTER_BUILD_DATE"),
      concat(year($"REL_BUILD_DATE"), lpad(month($"REL_BUILD_DATE"), 2, "0")).alias("REL_MONTH_BUILD_DATE"),
      year($"REL_BUILD_DATE").alias("REL_YEAR_BUILD_DATE"),
      $"REL_OEM_NAME",
      $"REL_ENGINE_PLANT",
      $"REL_USER_APPL_DESC",
      $"REL_ENGINE_NAME_DESC",
      $"REL_CMP_ENGINE_NAME",
      //$"REL_CMP_COVERAGE",
      $"REL_OEM_NORMALIZED_GROUP"
    ).distinct()

    val addtnlDim = addtnlDimBeforeMilageAndSource.
      withColumn("REL_MILAGE", lit(0)).
      withColumn("SOURCE", lit("REL"))

    val relOMData = groupedRel.join(addtnlDim, Seq("REL_ESN"), "left_outer").
      withColumn("REF_FTE_CMP_IS_TEST_ENGINE", lit(0))

    println("INFO::SAVING DATA IN TABLE:" + cfg(Constants.FET_DB) + "." + cfg(targetTblNameKey))
    Util.saveData(relOMData, cfg(Constants.FET_DB), cfg(targetTblNameKey), cfg(Constants.HDFS_PATH))
  }

  /**
    * Loads default set of columns from Incidents table
    *
    * @param incTblName [engine-platform-specific] incidents table
    * @return data
    */
  def loadIncidents(incTblName: String) = {
    val strSql: String =
      s"""
         |SELECT
         |GROUP_BY_ID,
         |GROUP_BY_DATE,
         |REL_ESN,
         |REL_FAILURE_DATE,
         |TEL_FAILURE_DATE,
         |REPORT_TYPE,
         |REL_CMP_COVERAGE,
         |REL_CMP_IN_SERVICE_DATE,
         |OCCURRENCE_DATE_TIME_LIST,
         |LATITUDE_LIST,
         |LONGITUDE_LIST,
         |ALTITUDE_LIST,
         |FAULT_CODE_LIST,
         |FAULT_CODE_DESCRIPTION_LIST,
         |DERATE_FLAG_LIST,
         |SHUTDOWN_FLAG_LIST,
         |FAULT_CODE_CATEGORY_LIST,
         |NONIM_OCCURRENCE_DATE_TIME_LIST,
         |NONIM_LATITUDE_LIST,
         |NONIM_LONGITUDE_LIST,
         |NONIM_ALTITUDE_LIST,
         |NONIM_FAULT_CODE_LIST,
         |NONIM_FAULT_CODE_DESCRIPTION_LIST,
         |NONIM_DERATE_FLAG_LIST,
         |NONIM_SHUTDOWN_FLAG_LIST,
         |NONIM_FAULT_CODE_CATEGORY_LIST,
         |REL_CMP_ENGINE_MILES_LIST,
         |REL_CMP_CLAIM_NUM_LIST,
         |REL_CMP_FAIL_CODE_LIST,
         |REL_CMP_FAILURE_PART_LIST,
         |REL_CMP_DEALER_NAME_LIST,
         |REL_CMP_CLAIM_DATE_LIST,
         |REL_CMP_FAIL_DATE_LIST,
         |REL_CMP_PART_NUM_LIST,
         |REL_CMP_BASE_OR_ATS_LIST,
         |REL_CMP_PAID_OPEN_LIST,
         |REL_CMP_DATA_PROVIDER_LIST,
         |REL_CMP_SUM_NET_AMOUNT,
         |REL_CMP_SUM_LABOR_HOURS
         |FROM $incTblName """.stripMargin
    val df = spark.sql(strSql)
    df
  }

  /**
    * Loads default set of columns from engine features table
    *
    * @param engFeatTblName [engine-platform-specific] engine features table (from Reliability)
    * @return data
    */
  def loadEngFeatures(engFeatTblName: String) = {
    val strSql: String =
      s"""
         |SELECT
         |ENGINE_DETAILS_ESN,
         |REL_SO_NUM,
         |REL_BUILD_DATE,
         |REL_EMISSION_LEVEL,
         |REL_ATS_TYPE,
         |REL_HORSEPOWER,
         |REL_USER_APPL_DESC,
         |REL_VEHICLE_TYPE,
         |REL_OEM_NAME,
         |REL_ENGINE_PLANT,
         |REL_IN_SERVICE_DATE_AVE_TIME_LAG,
         |REL_ENGINE_TYPE_DESC,
         |REL_WAREHOUSE,
         |REL_ACTUAL_IN_SERVICE_DATE,
         |REL_ENGINE_PLATFORM,
         |REL_ENGINE_GROUP_DESC,
         |REL_ABO_QUALITY_PERFORMANCE_GROUP,
         |REL_CCI_CHANNEL_SALES_Y_N,
         |REL_BUILD_YEAR,
         |REL_BUILD_MONTH,
         |REL_BUILD_DAY_OF_MONTH,
         |REL_BUILD_DAY_OF_WEEK,
         |REL_BUILT_ON_WEEKEND,
         |REL_NORMAL_EMISSION_LEVEL,
         |REL_ENGINE_NAME_DESC,
         |REL_CMP_TIME_DIFF_BUILDDATE_TO_INSERVICEDATE_DAYS,
         |REL_CMP_ENGINE_NAME,
         |REL_OEM_NORMALIZED_GROUP
         |FROM ${engFeatTblName}""".stripMargin

    val df = spark.sql(strSql)
    df
  }
}
