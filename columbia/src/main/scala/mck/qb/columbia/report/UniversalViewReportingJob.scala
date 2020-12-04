package mck.qb.columbia.report

import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO._
import mck.qb.library.{Job, Util}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.WrappedArray

object UniversalViewReportingJob extends Job with Transport {

  import spark.implicits._

  val myConf = AppConfig.getMyConfig(Constants.RPT_RPT_UV)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  // Get the fault/fail code description: quality_engine_features.uv_code_description
  private val uvCodeDesc = buildRefCodeDesc.coalesce(1).cache()

  case class Writer(df: DataFrame, tbl: String)

  override def run(): Unit = {

    val tblDfMap: Seq[String] = Seq("B", "D", "F", "L", "M", "NG", "X", "Z")
    val dmUnionDF = tblDfMap.map(detectMaster(_)).reduce(_.union(_)).coalesce(400).persist(StorageLevel.MEMORY_AND_DISK)

    val dfSeq = Seq(
      Writer(buildFailCodeDescription(dmUnionDF), cfg(Constants.UV_FAIL_CODE_TBL)), // Fail Code with Description UV_FAIL_CODE
      Writer(buildFaultCodeDescription(dmUnionDF), cfg(Constants.UV_FAULT_CODE_TBL)), // Fault Code with Description UV_FAULT_CODE
      Writer(buildPartDescription(dmUnionDF), cfg(Constants.UV_PART_NUMS_TBL)), // Part Number with Description PART_NUMS
      Writer(buildAllCodeDescription(dmUnionDF), cfg(Constants.UV_CODE_DESCRIPTION_TBL)), // Code Description UV_CODE_DESCRIPTION
      Writer(buildUV(dmUnionDF), cfg(Constants.UV_MASTER_TBL)), // UV Master UV_MASTER
      Writer(buildEsnPopulation(), cfg(Constants.UV_MASTER_ESN_ALL_TBL)), // ESN Population UV_MASTER_ESN_ALL
      Writer(buildTCDate(dmUnionDF), cfg(Constants.UV_TC_DATE_TBL)),// UV_TC_DATE
      Writer(buildVolume(),cfg(Constants.UV_BUL_VOL_TBL))//UV_SO_BUILD_VOLUME
      )

    dfSeq.foreach((writer: Writer) => {
      logger.warn("INFO :: Saving >>>>>> [%s] As [%s]".format(writer.df, writer.tbl))
      writer match {
        case Writer(df, tbl) => Util.saveData(df, cfg(Constants.REPORT_DB), tbl, cfg(Constants.HDFS_PATH))
      }
    })
  }

  /** *
    * Join the given DataFrame with uvCodeDesc based on the type of the joinKey
    *
    * @param df      DataFrame to join with uvCodeDec
    * @param joinKey joinExpression or Seq[String]
    * @tparam T Generic type parameter T
    * @return A DataFrame joined with uvCodeDesc based on it's type
    */
  def joinCodeDesc[T](df: DataFrame, joinKey: T = Seq("CODE", "REL_ENGINE_NAME_DESC")): DataFrame = {
    joinKey match {
      case j: Seq[_] => df.join(uvCodeDesc, j.mkString)
      case j: Column => df.join(uvCodeDesc, j)
    }
  }


  /** *
    * Build CODE_DESCRIPTION by doing a union of Fail and Fault codes.
    *
    * @param dmUnionDF Union of DetectMaster
    * @return
    */
  def buildAllCodeDescription(dmUnionDF: DataFrame): DataFrame = {
    def buildCodesDesc(df: DataFrame): Dataset[Row] = {
      df.withColumn("REL_MONTH_BUILD_DATE", concat(substring($"REL_BUILD_DATE", 1, 4), substring($"REL_BUILD_DATE", 6, 2))).join(uvCodeDesc, Seq("CODE", "REL_ENGINE_NAME_DESC")).
        select(
          $"ID",
          concat_ws("-", $"ID", $"PLATFORM") as "KEY_ID",
          $"FIRST_CODE",
          $"CODE",
          $"DESCRIPTION",
          $"REL_OEM_NORMALIZED_GROUP" as "REL_OEM_GROUP",
          $"REL_USER_APPL_DESC",
          concat_ws("-", $"CODE", $"REL_ENGINE_NAME_DESC", $"REL_USER_APPL_DESC", $"REL_MONTH_BUILD_DATE", $"REL_CMP_COVERAGE") as "W_KEY",
          $"REL_ENGINE_NAME_DESC",
          $"REL_BUILD_YEAR",
          $"REASON"
        )
        .distinct()
    }
    val ref_hl_code: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_HL_CODE_TBL)}")
    val failCodes = buildCodesDesc(
      dmUnionDF.withColumn("CODE", explode_outer($"REL_CMP_FAILURE_PART_LIST")).
        withColumn("FIRST_FLAG_TEMP", $"REL_CMP_FAILURE_PART_LIST" (0)).
        withColumn("FIRST_CODE", when($"FIRST_FLAG_TEMP" === $"CODE", lit(1)).otherwise(lit(0))).drop("FIRST_FLAG_TEMP")
    ).withColumn("CODE_SIGNAL",lit("fai"))

    val faultCodes = buildCodesDesc(
      dmUnionDF.withColumn("CODE", explode_outer($"Fault_Code_LIST")).union(dmUnionDF.withColumn("CODE", explode_outer($"nonIm_Fault_Code_LIST"))).
        //withColumn("CODE", regexp_replace($"CODE", lit(".0"), lit(""))).
        withColumn("FIRST_CODE", lit(0))
    ).withColumn("CODE_SIGNAL",lit("fau"))

    val allCodes: DataFrame = failCodes.union(faultCodes)
    allCodes.join(ref_hl_code,allCodes("CODE")===ref_hl_code("Fault_Code"),"left_outer")
      .drop("Fault_Code")
      .withColumn("Highlight",when($"Highlight"isNull,"0") otherwise lit(1) )
      .withColumnRenamed("Highlight","is_Highlight")
  }

  /** *
    * Build a DataFrame from REL_ENGINE_FEATURES to get the ESN Population Count.
    *
    * @return A DataFrame to save.
    */
  def buildEsnPopulation(): DataFrame = {
    load(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}",
      "ENGINE_DETAILS_ESN",
      "REL_BUILD_DATE",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_ENGINE_NAME_DESC",
      "REL_ENGINE_PLANT",
      "REL_USER_APPL_DESC",
      "FIRST_CONNECTION_TIME"
    ).
      //      filter($"REL_ENGINE_NAME_DESC".isin(engineNameDescList: _*)).
      withColumnRenamed("FIRST_CONNECTION_TIME","FIRST_CONNECTION_DATE").
      withColumn("REL_OEM_GROUP", upper($"REL_OEM_NORMALIZED_GROUP")).
      withColumn("REL_MONTH_BUILD_DATE", concat(substring($"REL_BUILD_DATE", 1, 4), substring($"REL_BUILD_DATE", 6, 2))).
      groupBy("REL_OEM_GROUP", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_ENGINE_PLANT", "REL_MONTH_BUILD_DATE").
      agg(
        countDistinct("ENGINE_DETAILS_ESN").alias("ESN_COUNT"),
        countDistinct("FIRST_CONNECTION_DATE").alias("ESN_CONNECTION_COUNT")
      )
  }



  /** *
    * Builds fail code and description mapping
    *
    * @param df DataFrame to create mapping
    * @return Resulting DataFrame
    */
  def buildFailCodeDescription(df: DataFrame): DataFrame =
    df.withColumn("CODE", explode($"REL_CMP_FAILURE_PART_LIST")).join(uvCodeDesc, Seq("CODE", "REL_ENGINE_NAME_DESC")).
      //      joinCodeDesc[Column](df.withColumn("CODE", explode($"REL_CMP_FAILURE_PART_LIST")), Seq("CODE", "REL_ENGINE_NAME_DESC")).
      select("ID", "PLATFORM", "CODE", "DESCRIPTION").withColumnRenamed("CODE", "FAIL_CODE")

  /** *
    * Builds fault code and description mapping
    *
    * @param df DataFrame to create mapping
    * @return Resulting DataFrame
    */
  def buildFaultCodeDescription(df: DataFrame): DataFrame = {

    val explode_final = df.select("ID", "PLATFORM", "fault_code_list", "REL_ENGINE_NAME_DESC", "Latitude_LIST", "longitude_list", "Occurrence_Date_Time_LIST").
      withColumn("newCols", ZipFaultCodeWithLOC($"fault_code_list", $"Latitude_LIST", $"longitude_list", $"Occurrence_Date_Time_LIST")).
      withColumn("newCol", explode($"newCols")).
      withColumn("Code", $"newCol" ("_1")).
      withColumn("Latitude", $"newCol" ("_2")).
      withColumn("Longitude", $"newCol" ("_3")).
      withColumn("Occurrence_Date_Time", $"newCol" ("_4")).
      drop("newCol")

    val explode_final_n = df.select("ID", "PLATFORM", "nonIm_Fault_Code_LIST", "REL_ENGINE_NAME_DESC", "nonIm_Latitude_LIST", "nonIm_Longitude_LIST", "nonIm_Occurrence_Date_Time_LIST").
      withColumn("newCols", ZipFaultCodeWithLOC($"nonIm_Fault_Code_LIST", $"nonIm_Latitude_LIST", $"nonIm_Longitude_LIST", $"nonIm_Occurrence_Date_Time_LIST")).
      withColumn("newCol", explode($"newCols")).
      withColumn("Code", $"newCol" ("_1")).
      withColumn("Latitude", $"newCol" ("_2")).
      withColumn("Longitude", $"newCol" ("_3")).
      withColumn("Occurrence_Date_Time", $"newCol" ("_4")).
      drop("newCol")

    val tmp_final_0: DataFrame = explode_final.union(explode_final_n).withColumn("CODE", regexp_replace($"CODE", lit(".0"), lit("")))
    tmp_final_0.createOrReplaceTempView("tmp_final_0")
    val tmp_final: DataFrame = spark.sql(
      s"""
         |SELECT * FROM
         | (
         | SELECT *,
         | ROW_NUMBER() OVER(PARTITION BY ID,PLATFORM,Code ORDER BY Occurrence_Date_Time ) RANK
         | FROM tmp_final_0
         | )
         | WHERE RANK=1
          """.stripMargin).drop("rank", "Occurrence_Date_Time")

    val result_1: DataFrame =
      tmp_final.join(uvCodeDesc, Seq("CODE", "REL_ENGINE_NAME_DESC"))
        .withColumnRenamed("CODE", "FAULT_CODE")
        .withColumn("Latitude",bround($"Latitude",3))
        .withColumn("Longitude",bround($"Longitude",3))
        .select("ID", "PLATFORM", "FAULT_CODE", "DESCRIPTION", "Latitude", "Longitude")
    //result_1.distinct()
    val ref_hl_code: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_HL_CODE_TBL)}")
        .withColumnRenamed("FAULT_CODE","REF_FAULT_CODE")

    val result_df = result_1
      .join(ref_hl_code, result_1("FAULT_CODE") === ref_hl_code("REF_FAULT_CODE"), "left_outer")
      .drop("REF_FAULT_CODE")
      .withColumn("Highlight", when($"Highlight" isNull, 0) otherwise lit(1))
      .withColumnRenamed("Highlight", "is_Highlight")
      .drop("CODE")

    //add province and city
    //Use the Window Function to filter multiple different provinces and cities at the same Latitude and Longitude
    val ref_tel_gdb: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_TEL_GDB_TBL)}")
      .select("LGT", "LAT", "PROVINCE", "CITY")
      .withColumn("LGT", bround($"LGT", 3))
      .withColumn("LAT", bround($"LAT", 3))
      .withColumn("rank", row_number().over(Window.partitionBy("LGT", "LAT").orderBy("PROVINCE", "CITY")))
      .where($"rank" === 1)
      .drop("rank")

    result_df
      .join(ref_tel_gdb, result_df("Longitude") === ref_tel_gdb("LGT") && result_df("Latitude") === ref_tel_gdb("LAT"), "left")
      .drop("LGT","LAT")
  }

  def ZipFaultCodeWithLOC: UserDefinedFunction = udf {
    (fault_code_list: Seq[String], latitude_list: Seq[String], longitude_list: Seq[String], nonim_occurrence_date_time_list: Seq[String]) => {
      val values = fault_code_list.zip(latitude_list.zip(longitude_list)).zip(nonim_occurrence_date_time_list)
      values.map(value => (value._1._1, value._1._2._1, value._1._2._2, value._2))
    }
  }

  /** *
    * Builds part numbers and description mapping
    *
    * @param df DataFrame to create mapping
    * @return Resulting DataFrame
    */
  def buildPartDescription(df: DataFrame): DataFrame = {
    val explode_PART_NUM = df.select("ID", "PLATFORM", "REL_CMP_PART_NUM_LIST")
      .withColumn("PART_NUM", explode($"REL_CMP_PART_NUM_LIST"))
      .withColumn("new_id", monotonically_increasing_id)
    val explode_PART_DESC = df.select("REL_CMP_FAILURE_PART_LIST")
      .withColumn("PART_DESC", explode($"REL_CMP_FAILURE_PART_LIST"))
      .withColumn("new_id", monotonically_increasing_id)
    explode_PART_NUM.join(explode_PART_DESC, Seq {
      "new_id"
    }, "left")
      .select("ID", "PLATFORM", "PART_NUM", "PART_DESC")
  }

  /** *
    * Select from DetectMaster model input based on engine type
    *
    * @param engineType Engine Type
    * @return DataFrame DetectMaster
    */
  def detectMaster(engineType: String): DataFrame = {
    load(
      s"""
         |SELECT CAST(ID AS INTEGER), REL_ESN,
         |GROUP_BY_ID,
         |GROUP_BY_DATE,
         |REL_FAILURE_DATE,
         |TEL_FAILURE_DATE,
         |Report_Type,
         |REL_CMP_COVERAGE,
         |REL_CMP_IN_SERVICE_DATE,
         |Occurrence_Date_Time_LIST,
         |Latitude_LIST,
         |Longitude_LIST,
         |Altitude_LIST,
         |Fault_Code_LIST,
         |Fault_Code_Description_LIST,
         |Derate_Flag_LIST,
         |Shutdown_Flag_LIST,
         |Fault_Code_Category_LIST,
         |nonIm_Occurrence_Date_Time_LIST,
         |nonIm_Latitude_LIST,
         |nonIm_Longitude_LIST,
         |nonIm_Altitude_LIST,
         |nonIm_Fault_Code_LIST,
         |nonIm_Fault_Code_Description_LIST,
         |nonIm_Derate_Flag_LIST,
         |nonIm_Shutdown_Flag_LIST,
         |nonIm_Fault_Code_Category_LIST,
         |REL_CMP_ENGINE_MILES_LIST,
         |REL_CMP_ENGINE_WORK_HOUR_LIST,
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
         |REL_CMP_FAILURE_MODE_LIST,
         |REL_CMP_SUM_NET_AMOUNT,
         |REL_CMP_SUM_LABOR_HOURS,
         |REL_CMP_CAUSE_LIST,
         |REL_CMP_COMPLAINT_LIST,
         |REL_CMP_CORRECTION_LIST,
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
         |REL_Normal_EMISSION_LEVEL,
         |REL_ENGINE_NAME_DESC,
         |REL_CMP_TIME_DIFF_BUILDDATE_TO_INSERVICEDATE_DAYS,
         |REL_CMP_ENGINE_NAME,
         |REL_OEM_NORMALIZED_GROUP,
         |CMP_FAULT_CODE,
         |CMP_MILEAGE_LIST,
         |FAULT_CODE_REASON_LIST,
         |NONIM_FAULT_CODE_REASON_LIST,
         |CALIBRATION_VERSION_LIST,
         |SOURCE
         |FROM ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.FULL_FET_TBL)}_${engineType}
      """.stripMargin).
      withColumn("BUILD_QUARTER", concat(substring($"REL_BUILD_DATE", 1, 4), lit("-Q"), quarter($"REL_BUILD_DATE"))).
      withColumn("PLATFORM", lit(engineType))
  }

  /** *
    * Build Universal View dataframe with additional computed columns
    *
    * @param dfALL DataFrame to create result
    * @return DataFrame to save
    */
  def buildUV(dfALL: DataFrame): DataFrame = {

    def applyEWCol(df: DataFrame): DataFrame = {
      val fullfet_nsvi_df: DataFrame = df
        .select("GROUP_BY_ID",
          "GROUP_BY_DATE",
          "REL_ENGINE_PLATFORM",
          "REL_FAILURE_DATE",
          "REL_CMP_FAILURE_PART_LIST",
          "Fault_Code_LIST",
          "nonIm_Fault_Code_LIST")
        .where(upper($"REL_Normal_EMISSION_LEVEL") === "NSVI")
      val fullfet_tfd_df: DataFrame = df
        .select("GROUP_BY_ID", "TEL_FAILURE_DATE")
        .where(upper($"REL_Normal_EMISSION_LEVEL") === "NSVI")
        .withColumnRenamed("TEL_FAILURE_DATE","TEL_FAILURE_DATE_TMP")

      val ew_nsvi_df: DataFrame = fullfet_nsvi_df
        .join(fullfet_tfd_df,
          fullfet_nsvi_df("GROUP_BY_ID") === fullfet_tfd_df("GROUP_BY_ID")
              && datediff(fullfet_nsvi_df("REL_FAILURE_DATE"), fullfet_tfd_df("TEL_FAILURE_DATE_TMP")) >= 0
              && datediff(fullfet_nsvi_df("REL_FAILURE_DATE"), fullfet_tfd_df("TEL_FAILURE_DATE_TMP")) <= 14
            )
        .drop(fullfet_tfd_df.col("GROUP_BY_ID"))
        .drop("TEL_FAILURE_DATE_TMP")

      val explode_fc_df: DataFrame = ew_nsvi_df
        .withColumn("FAULT_CODE", explode_outer($"Fault_Code_LIST"))
        .union(
          ew_nsvi_df.withColumn("FAULT_CODE", explode_outer($"nonIm_Fault_Code_LIST")))

      val ew_fault_code_df: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_NSVI_EW_FC_TBL)}")
        .withColumnRenamed("ENGINE_PLATFORM", "REL_ENGINE_PLATFORM")
      val ew_fcfiltered_df: DataFrame = explode_fc_df
        .join(ew_fault_code_df, Seq("FAULT_CODE", "REL_ENGINE_PLATFORM"))
        .drop("EMISSION_FAULT_CODE")
        .withColumn("REL_CMP_FAILURE_PART", explode_outer($"REL_CMP_FAILURE_PART_LIST"))

      val failure_part_df: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_NSVI_EW_FP_TBL)}")
        .where(upper($"EW_PART") === "Yes".toUpperCase())
      val ew_fpfiltered_df1: DataFrame = ew_fcfiltered_df
        .join(failure_part_df, upper(ew_fcfiltered_df("REL_CMP_FAILURE_PART")) === upper(failure_part_df("FAILURE_PART")))
        .drop("FAILURE_PART")
        .drop("EW_PART")

      val fourteen_fp_seq = Seq("增压器", "Turbocharger", "Turbo",
        "进气节流阀", "Intake Air Throttle Actuator", "IAT",
        "高压油轨", "高压共轨", "Fuel Lines", "Common Rail",
        "喷油器", "Injector",
        "喷油泵", "燃油泵", "Fuel Pump",
        "催化转化器", "排期处理器", "后处理总成", "排气后处理器", "SCR", "EGP",
        "氧化催化", "Diesel Oxidation Catalyst", "DOC",
        "颗粒捕捉器", "颗粒捕集器", "Diesel Particulate Filter", "DPF",
        "尿素喷射泵", "尿素泵", "后处理给料", "Dosing Unit", "Supply Module", "Doser", "Dosing Module",
        "反应剂质量传感器", "尿素质量传感器", "尿素传感", "尿素罐传感", "DEF level sensor assembly (UQS)", "UQS",
        "温度传感器 ", "Aftertreatment Temperature Sensors",
        "氮氧", "NOx Sensor", "Nox",
        "压差传感器", "Diesel Particulate Filter Differential Pressure Sensor", "Delta P Sensor", "DP sensor",
        "电控模块", "电控单元", "控制模块", "Engine Control Module", "ECM", "ECU")
      val fourteen_fp_df: DataFrame = sc.makeRDD(fourteen_fp_seq).toDF("FAILURE_PART")
      val ew_fpfiltered_df2: DataFrame = ew_fcfiltered_df
        .join(fourteen_fp_df,upper(ew_fcfiltered_df("REL_CMP_FAILURE_PART")).contains(upper(fourteen_fp_df("FAILURE_PART"))))
        .drop("FAILURE_PART")

      val ew_result_df: DataFrame = ew_fpfiltered_df1.union(ew_fpfiltered_df2)
        .withColumn("EMISSION_WARRANTY", lit("Y"))
        .select("GROUP_BY_ID", "GROUP_BY_DATE", "EMISSION_WARRANTY")
        .distinct()

      df.join(ew_result_df, Seq("GROUP_BY_ID", "GROUP_BY_DATE"), "left")

    }

    val ew_df: DataFrame = applyEWCol(dfALL)

    val df1 = ew_df.
      withColumn("REL_CMP_FAILURE_PART_LIST", concat_ws(",", $"REL_CMP_FAILURE_PART_LIST")).
      withColumn("REL_CMP_ABO_FAILURE_PART_LIST", concat_ws(",", $"REL_CMP_ABO_FAILURE_PART_LIST")).
      withColumn("Fault_Code_LIST", col("Fault_Code_LIST").cast("array<string>")).
      withColumn("Fault_Code_LIST", Util.ListReduceCount2String($"Fault_Code_LIST", lit("*"), lit(","))).
      //withColumn("Fault_Code_LIST", concat_ws(",", $"Fault_Code_LIST")).
      withColumn("nonIm_Fault_Code_LIST", col("nonIm_Fault_Code_LIST").cast("array<string>")).
      withColumn("nonIm_Fault_Code_LIST", Util.ListReduceCount2String($"nonIm_Fault_Code_LIST", lit("*"), lit(","))).
      //withColumn("nonIm_Fault_Code_LIST", concat_ws(",", $"nonIm_Fault_Code_LIST")).
      withColumn("REL_CMP_ENGINE_MILES_LIST", $"REL_CMP_ENGINE_MILES_LIST" (0)).
      withColumn("REL_CMP_PART_NUM_LIST", concat_ws(",", $"REL_CMP_PART_NUM_LIST")).
      withColumn("REL_CMP_CAUSE_LIST", concat_ws(",", $"REL_CMP_CAUSE_LIST")).
      withColumn("REL_CMP_COMPLAINT_LIST", concat_ws(",", $"REL_CMP_COMPLAINT_LIST")).
      withColumn("REL_CMP_CORRECTION_LIST", concat_ws(",", $"REL_CMP_CORRECTION_LIST")).
      withColumn("rel_cmp_dealer_name_list", Util.stringListToSet($"rel_cmp_dealer_name_list")).
      withColumn("rel_cmp_dealer_name_list", Util.selectLast($"rel_cmp_dealer_name_list"))


    //利用参考表查找city name, province name
    val refDealer = buildRefDealerLocation.coalesce(1)
    val broadcast: Broadcast[Dataset[Row]] = spark.sparkContext.broadcast(refDealer)

    //分批处理解决数据倾斜
    val null_list_df: Dataset[Row] = df1.filter($"REL_CMP_DEALER_NAME_LIST" isNull)
      .withColumn("CITY", lit(""))
      .withColumn("PROVINCE", lit(""))

    val notnull_list_df: Dataset[Row] = df1.filter($"REL_CMP_DEALER_NAME_LIST" isNotNull)
    val joined_df: DataFrame = notnull_list_df.join(broadcast.value, df1("REL_CMP_DEALER_NAME_LIST") === refDealer("SERVICEDEALER"), "leftouter").
      drop("SERVICEDEALER")

    val df2: Dataset[Row] = null_list_df.unionByName(joined_df).
      withColumnRenamed("CITY", "REL_CMP_CLM_CITY_NAME").
      withColumnRenamed("PROVINCE", "REL_CMP_CLM_PROVINCE_NAME")

    val dfResult = df2.
      //column distinct
      withColumn("Occurrence_Date_Time_LIST", Util.stringListToSet($"Occurrence_Date_Time_LIST")).
      withColumn("latitude_list", Util.stringListToSet($"latitude_list")).
      withColumn("longitude_list", Util.stringListToSet($"longitude_list")).
      withColumn("altitude_list", Util.stringListToSet($"altitude_list")).
      withColumn("derate_flag_list", Util.stringListToSet($"derate_flag_list")).
      withColumn("shutdown_flag_list", Util.stringListToSet($"shutdown_flag_list")).
      withColumn("fault_code_category_list", Util.stringListToSet($"fault_code_category_list")).
      withColumn("nonim_occurrence_date_time_list", Util.stringListToSet($"nonim_occurrence_date_time_list")).
      withColumn("nonim_latitude_list", Util.stringListToSet($"nonim_latitude_list")).
      withColumn("nonim_longitude_list", Util.stringListToSet($"nonim_longitude_list")).
      withColumn("nonim_altitude_list", Util.stringListToSet($"nonim_altitude_list")).
      withColumn("nonim_fault_code_description_list", Util.stringListToSet($"nonim_fault_code_description_list")).
      withColumn("nonim_derate_flag_list", Util.stringListToSet($"nonim_derate_flag_list")).
      withColumn("nonim_shutdown_flag_list", Util.stringListToSet($"nonim_shutdown_flag_list")).
      withColumn("nonim_fault_code_category_list", Util.stringListToSet($"nonim_fault_code_category_list")).
      withColumn("rel_cmp_claim_num_list", Util.stringListToSet($"rel_cmp_claim_num_list")).
      withColumn("rel_cmp_fail_code_list", Util.stringListToSet($"rel_cmp_fail_code_list")).
      //      withColumn("rel_cmp_dealer_name_list",Util.stringListToSet($"rel_cmp_dealer_name_list")).
      withColumn("rel_cmp_claim_date_list", Util.stringListToSet($"rel_cmp_claim_date_list")).
      withColumn("rel_cmp_base_or_ats_list", Util.stringListToSet($"rel_cmp_base_or_ats_list")).
      withColumn("rel_cmp_paid_open_list", Util.stringListToSet($"rel_cmp_paid_open_list")).
      withColumn("rel_cmp_data_provider_list", Util.stringListToSet($"rel_cmp_data_provider_list")).
      withColumn("rel_cmp_failure_mode_list",Util.stringListToSet($"rel_cmp_failure_mode_list")).
      withColumn("cmp_fault_code", Util.stringListToSet($"cmp_fault_code")).
      withColumn("fault_code_description_list", Util.stringListToSet($"fault_code_description_list")).
      withColumn("FAULT_CODE_REASON_LIST",Util.stringListToSet($"FAULT_CODE_REASON_LIST")).
      withColumn("NONIM_FAULT_CODE_REASON_LIST",Util.stringListToSet($"NONIM_FAULT_CODE_REASON_LIST")).
      withColumn("CALIBRATION_VERSION_LIST",Util.stringListToSet($"CALIBRATION_VERSION_LIST")).

      withColumn("TEL_Fault_Code_LATITUDE_LIST", $"Latitude_LIST" (0)).
      withColumn("TEL_Fault_Code_Longitude_LIST", $"Longitude_LIST" (0)).


      withColumn("Occurrence_Date_Time_LIST", concat_ws(",", $"Occurrence_Date_Time_LIST")).
      withColumn("latitude_list", concat_ws(",", $"latitude_list")).
      withColumn("longitude_list", concat_ws(",", $"longitude_list")).
      withColumn("altitude_list", concat_ws(",", $"altitude_list")).
      withColumn("derate_flag_list", concat_ws(",", $"derate_flag_list")).
      withColumn("shutdown_flag_list", concat_ws(",", $"shutdown_flag_list")).
      withColumn("fault_code_category_list", concat_ws(",", $"fault_code_category_list")).
      withColumn("nonim_occurrence_date_time_list", concat_ws(",", $"nonim_occurrence_date_time_list")).
      withColumn("nonim_latitude_list", concat_ws(",", $"nonim_latitude_list")).
      withColumn("nonim_longitude_list", concat_ws(",", $"nonim_longitude_list")).
      withColumn("nonim_altitude_list", concat_ws(",", $"nonim_altitude_list")).
      withColumn("nonim_fault_code_description_list", concat_ws(",", $"nonim_fault_code_description_list")).
      withColumn("nonim_derate_flag_list", concat_ws(",", $"nonim_derate_flag_list")).
      withColumn("nonim_shutdown_flag_list", concat_ws(",", $"nonim_shutdown_flag_list")).
      withColumn("nonim_fault_code_category_list", concat_ws(",", $"nonim_fault_code_category_list")).
      withColumn("rel_cmp_claim_num_list", concat_ws(",", $"rel_cmp_claim_num_list")).
      withColumn("rel_cmp_fail_code_list", concat_ws(",", $"rel_cmp_fail_code_list")).
      withColumn("rel_cmp_dealer_name_list", concat_ws(",", $"rel_cmp_dealer_name_list")).
      withColumn("rel_cmp_claim_date_list", concat_ws(",", $"rel_cmp_claim_date_list")).
      withColumn("rel_cmp_base_or_ats_list", concat_ws(",", $"rel_cmp_base_or_ats_list")).
      withColumn("rel_cmp_paid_open_list", concat_ws(",", $"rel_cmp_paid_open_list")).
      withColumn("rel_cmp_data_provider_list", concat_ws(",", $"rel_cmp_data_provider_list")).
      withColumn("rel_cmp_failure_mode_list",concat_ws(",",$"rel_cmp_failure_mode_list")).
      withColumn("cmp_fault_code", concat_ws(",", $"cmp_fault_code")).
      withColumn("fault_code_description_list", concat_ws(",", $"fault_code_description_list")).
      withColumn("FAULT_CODE_REASON_LIST",concat_ws(",",$"FAULT_CODE_REASON_LIST")).
      withColumn("NONIM_FAULT_CODE_REASON_LIST",concat_ws(",",$"NONIM_FAULT_CODE_REASON_LIST")).
      withColumn("CALIBRATION_VERSION_LIST",concat_ws(",",$"CALIBRATION_VERSION_LIST")).
      // TODO: Verify the logic here
      //withColumn("MAB", Util.monthsBetween(coalesce($"REL_FAILURE_DATE", $"TEL_FAILURE_DATE"), $"REL_BUILD_MONTH")).
      withColumn("MAB", months_between(date_format(coalesce($"REL_FAILURE_DATE", $"TEL_FAILURE_DATE"),"yyyy-MM"), date_format($"REL_BUILD_DATE","yyyy-MM"))).
      withColumn("MAB_RANGE",
        when($"MAB" <= 6, lit($"MAB")).
            otherwise(when($"MAB" <= 9, lit("9")).
              otherwise(when($"MAB" <= 12, lit("12")).
                otherwise(when($"MAB" <= 15, lit("15")).
                  otherwise(when($"MAB" <= 18, lit("18")).
                    otherwise(when($"MAB" <= 21, lit("21")).
                      otherwise(when($"MAB" <= 24, lit("24")).
                      otherwise(lit("25")))))))))
      .drop("TEL_FAULT_CODE_LATITUDE_LIST", "TEL_FAULT_CODE_LONGITUDE_LIST")
      .where($"MAB_RANGE" >= 0)
      .withColumn("rel_build_date", concat_ws(",", $"rel_build_date"))
      .withColumn("rel_in_service_date_ave_time_lag", concat_ws(",", $"rel_in_service_date_ave_time_lag"))
      .withColumn("rel_actual_in_service_date", concat_ws(",", $"rel_actual_in_service_date"))
      .withColumn("rel_cmp_in_service_date", concat_ws(",", $"rel_cmp_in_service_date"))
      //translate date to string
      .withColumn("rel_failure_date", concat_ws(",", $"rel_failure_date"))
      .withColumn("tel_failure_date", concat_ws(",", $"tel_failure_date"))
      .withColumn("group_by_date", concat_ws(",", $"group_by_date"))
      //translate boolean to string
      .withColumn("rel_built_on_weekend", concat_ws(",", $"rel_built_on_weekend"))
      .withColumn("fault_code_list", $"fault_code_list".substr(1, 1000))
      .withColumn("nonim_fault_code_list", $"nonim_fault_code_list".substr(1, 1000))
      .withColumn("occurrence_date_time_list", $"occurrence_date_time_list".substr(1, 1000))
      .withColumn("latitude_list", $"latitude_list".substr(1, 1000))
      .withColumn("altitude_list", $"altitude_list".substr(1, 1000))
      .withColumn("longitude_list", $"longitude_list".substr(1, 1000))
      .withColumn("nonim_occurrence_date_time_list", $"nonim_occurrence_date_time_list".substr(1, 1000))
      .withColumn("nonim_latitude_list", $"nonim_latitude_list".substr(1, 1000))
      .withColumn("nonim_longitude_list", $"nonim_longitude_list".substr(1, 1000))
      .withColumn("nonim_altitude_list", $"nonim_altitude_list".substr(1, 1000))
      .withColumn("ENGINE_NAME", Util.subStringWithSpec($"REL_ENGINE_NAME_DESC", lit("_")))

    dfResult
  }

  private def buildRefCodeDesc = {
    val ref_fail_part_code_df: DataFrame = spark.table(s"""${cfg(Constants.REF_DB)}.${cfg(Constants.REF_FP_DESC_TBL)}""")
      .withColumn("REASON",lit("NULL"))
    val ref_telfc_desc_df: DataFrame = spark.table(s"""${cfg(Constants.REF_DB)}.${cfg(Constants.REF_TFC_DESC_TBL)}""")
    ref_fail_part_code_df
      .union(ref_telfc_desc_df).
      select("CODE", "REL_ENGINE_NAME_DESC", "DESCRIPTION","REASON")
  }

  private def buildRefDealerLocation = {
    val df = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_DLR_LOC_TBL)}").
      select("SERVICEDEALER", "PROVINCE", "CITY")
    df
  }

  /**
    * This function is build the uv_tc_date table
    * Nesting of functions occurs for the sake of module integrity
    * @param df
    * @return
    */
  def buildTCDate(df: DataFrame): DataFrame = {

    def TCDateBuilder(source: String, tc_date: String, code_list: String): DataFrame = {
      spark.catalog.dropTempView("tmp_table")
      df.select("REL_BUILD_DATE",
        tc_date,
        code_list,
        "REL_CMP_COVERAGE",
        "REL_SO_NUM",
        "REL_USER_APPL_DESC",
        "REL_BUILD_YEAR",
        "REL_ENGINE_NAME_DESC",
        "REL_ENGINE_PLANT",
        "REL_OEM_NORMALIZED_GROUP",
        "PLATFORM",
        "CMP_MILEAGE_LIST")
        .filter(size(col(code_list)) > 0)
        .filter(if (source.equalsIgnoreCase("tel")) $"REL_BUILD_DATE" < $"TEL_FAILURE_DATE" else $"REL_BUILD_DATE" < $"REL_FAILURE_DATE")
        .withColumnRenamed(tc_date, "TC_DATE")
        .withColumnRenamed("CMP_MILEAGE_LIST","TC_MILEAGE")
        .withColumn("REL_BUILD_MONTH", date_format($"REL_BUILD_DATE", "yyyyMM"))
        .withColumn("CODE", explode(Util.stringListToSet(col(code_list))))
        .drop(code_list, "REL_BUILD_DATE")
        .createOrReplaceTempView("tmp_table")

      spark.sql(
        """
          |SELECT * FROM (
          |SELECT *,
          |ROW_NUMBER() OVER(PARTITION BY CODE, REL_USER_APPL_DESC, REL_BUILD_YEAR, REL_CMP_COVERAGE, REL_BUILD_MONTH, REL_ENGINE_NAME_DESC, REL_ENGINE_PLANT, REL_OEM_NORMALIZED_GROUP, PLATFORM ORDER BY TC_DATE ASC) RANK
          |FROM tmp_table
          |)
          |WHERE RANK = 1
        """.stripMargin
      )
        .drop("RANK")
        .withColumn("SOURCE", lit(source))
    }

    TCDateBuilder("Tel", "TEL_FAILURE_DATE", "FAULT_CODE_LIST")
      .union(TCDateBuilder("Tel", "TEL_FAILURE_DATE", "NONIM_FAULT_CODE_LIST"))
      .union(TCDateBuilder("Rel", "REL_FAILURE_DATE", "REL_CMP_FAILURE_PART_LIST"))
      .filter(($"code" isNotNull).and($"code" =!= ""))
      .withColumn("TC_DATE",$"TC_DATE".cast("String"))
      .withColumn("ENGINE_NAME",Util.subStringWithSpec($"REL_ENGINE_NAME_DESC",lit("_")))
      .select("CODE", "REL_USER_APPL_DESC", "REL_BUILD_YEAR", "REL_ENGINE_NAME_DESC", "REL_ENGINE_PLANT", "REL_OEM_NORMALIZED_GROUP", "REL_CMP_COVERAGE", "PLATFORM", "REL_BUILD_MONTH", "SOURCE", "TC_DATE", "REL_SO_NUM","ENGINE_NAME","TC_MILEAGE")
  }

  def buildVolume():DataFrame={
    val rel_eng_fea_df = spark.table(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}")

    val dfresult = rel_eng_fea_df
      .select(
        "REL_SO_NUM",
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_ENGINE_PLANT",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_BUILD_DATE")
      .withColumn("REL_BUILD_MONTH",concat(substring($"REL_BUILD_DATE",1,4),substring($"REL_BUILD_DATE",6,2)))
      .drop("REL_BUILD_DATE")
      .groupBy(
        "REL_SO_NUM",
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_ENGINE_PLANT",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_BUILD_MONTH"
          )
      .count()
      .withColumnRenamed("count","BUILD_VOLUME")
    dfresult
  }


}
