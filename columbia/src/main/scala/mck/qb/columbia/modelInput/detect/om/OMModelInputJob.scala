package mck.qb.columbia.modelInput.detect.om

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

abstract class OMModelInputJob extends Job with Transport {

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_DETECT_OM)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  def getTargetDB: String

  def getEngType: String

  override def run(): Unit = {

    case class PrgGrpCfg(inTbl: String, filterExpr: String, outTblSfx: String, flag:Int=0)

    val tblDfMap: Seq[PrgGrpCfg] = Seq(
      PrgGrpCfg(inTbl = Constants.OCM_REL_MTR_WARRANTY_TBL, filterExpr = "REL_CMP_COVERAGE == 'Warranty'", outTblSfx = "WARRANTY"),
      PrgGrpCfg(inTbl = Constants.OCM_REL_MTR_POLICY_TBL, filterExpr = "REL_CMP_COVERAGE == 'Policy'", outTblSfx = "POLICY"),
      PrgGrpCfg(inTbl = Constants.OCM_REL_MTR_CAMPAIGN_TRP_TBL, filterExpr = "REL_CMP_COVERAGE == 'Campaign_TRP'", outTblSfx = "CAMPAIGN_TRP"),
      PrgGrpCfg(inTbl = Constants.OCM_REL_MTR_FIELD_TEST_TBL, filterExpr = "REL_CMP_COVERAGE == 'Field_test'", outTblSfx = "FIELD_TEST"),
      PrgGrpCfg(inTbl = Constants.OCM_REL_MTR_EMISSION_TBL, filterExpr = "REL_CMP_COVERAGE == 'Emission'", outTblSfx = "EMISSION"),
      //todo:
      PrgGrpCfg(inTbl = Constants.OCM_TEL_MTR_FLD_TBL, filterExpr = "1=1", outTblSfx = "TEL_ALL",1),
      PrgGrpCfg(inTbl = Constants.OCM_REL_MTR_ALL_TBL, filterExpr = "REL_CMP_COVERAGE IN ('Warranty','Policy','Campaign_TRP','Field_test','Emission')", outTblSfx = "ALL")
    )

    for (pgCfg <- tblDfMap) {
      if (pgCfg.flag==0){
      val inTbl = pgCfg.inTbl // "OCM_REL_MASTER"
      val filt = pgCfg.filterExpr
      val resultMITbl = s"${cfg(Constants.MODEL_IP_TBL)}_${pgCfg.outTblSfx}_${getEngType}" //"OCCURRENCE_MNTR_WRNTY_B"
      println("Info: Input_table" + inTbl, "  Filter Condition:" + filt, "  Output_Table_Name:" + resultMITbl)
      val result: Dataset[Row] = buildOmModelInput(cfg(inTbl), filt)

      Util.saveData(result, getTargetDB, resultMITbl, cfg(Constants.HDFS_PATH))
      println("Info: DATA GETTING SAVED IN: " + getTargetDB + "." + resultMITbl)
      }else if (pgCfg.flag==1){
        tmpFuncForFieldTest(pgCfg)
      }
    }

   def tmpFuncForFieldTest(pgCfg:PrgGrpCfg) ={
      val inTbl = pgCfg.inTbl // "OCM_REL_MASTER"
      val filt = pgCfg.filterExpr
      val resultMITbl = s"${cfg(Constants.MODEL_IP_TBL)}_${pgCfg.outTblSfx}_${getEngType}" //"OCCURRENCE_MNTR_WRNTY_B"
      println("Info: Input_table" + inTbl, "  Filter Condition:" + filt, "  Output_Table_Name:" + resultMITbl)

     val result: Dataset[Row] =
       buildOmModelInput(cfg(Constants.OCM_TEL_MTR_FLD_TBL), filt)
       .union(buildOmModelInput(cfg(Constants.OCM_TEL_MTR_NON_FLD_TBL), filt))
     Util.saveData(result, getTargetDB, resultMITbl, cfg(Constants.HDFS_PATH))
     println("Info: DATA GETTING SAVED IN: " + getTargetDB + "." + resultMITbl)
   }

  }

  private def buildOmModelInput(omRelMstTbl: String, prgGrpFilter: String) = {
    import spark.implicits._

    /**
      * 计算COUNT_NBR
      **/
    //3 different table OCM_REL_MASTER
    val inc_all_source_DF0 =
    load(s"select * from ${cfg(Constants.FET_DB)}.${omRelMstTbl}").
      filter($"CODE" =!= "NA").
      filter($"REF_FTE_CMP_IS_TEST_ENGINE" === 0)
    val inc_all_source_DF = Util.filterEnginesByType(cfg, inc_all_source_DF0, getEngType, esnColName = "REL_ESN")

    val dt_fltDF = inc_all_source_DF.
      withColumn("TUE_TO", date_sub(next_day(current_timestamp(), "Wed"), 7)).
      withColumn("TUE_2FROM", date_sub(next_day(current_timestamp(), "Wed"), 730)).
      withColumn("TUE_1FROM", date_sub(next_day(current_timestamp(), "Wed"), 365))

    val inc_all2yr_df = dt_fltDF.filter($"CALC_DATE".gt($"TUE_2FROM") && $"CALC_DATE".lt($"TUE_TO")).drop("TUE_TO", "TUE_2FROM", "TUE_1FROM")

    val incident_DF = inc_all2yr_df.
      select(
        "CODE",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_QUARTER_BUILD_DATE",
        "REL_MONTH_BUILD_DATE",
        "REL_ENGINE_NAME_DESC",
        "REL_USER_APPL_DESC",
        "REL_CMP_ENGINE_NAME",
        "SOURCE",
        "CALC_DATE",
        "COUNT").withColumn("REL_YEAR_BUILD_DATE", substring($"REL_MONTH_BUILD_DATE", 1, 4)).cache()

    //以前是 对于每一个code在这个calc date里面的esn数量，
    //该故障码对于发动机在这个calculate date里面的数量之和
    val inc_count_nbr_df = incident_DF.groupBy(
      "CODE",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_QUARTER_BUILD_DATE",
      "REL_MONTH_BUILD_DATE",
      "REL_ENGINE_NAME_DESC",
      "REL_USER_APPL_DESC",
      "REL_CMP_ENGINE_NAME",
      "SOURCE",
      "CALC_DATE",
      "REL_YEAR_BUILD_DATE").
      agg(sum("COUNT").as("COUNT_NBR"))

    /**
      * 计算NET_AMOUNT
      **/
    val relClaimDFAll = load(
      s"""
         |SELECT
         |    ESN_NEW AS ESN,
         |    date(Failure_Date) as REL_FAILURE_DATE,
         |    Failure_Part as CODE,
         |    Total_Cost,
         |    Coverage
         |    FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_CLM_DTL_TBL)}
      """.stripMargin).
      withColumn("REL_CMP_COVERAGE", Util.getNormCoverage($"Coverage")).
      filter(prgGrpFilter)

    val telDFAll = load(
      s"""
         |SELECT
         |     Engine_Serial_Number as ESN,
         |     date(Occurrence_Date_Time) as REL_FAILURE_DATE,
         |     Fault_Code as CODE,
         |     '0' as Total_Cost
         |FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.TEL_FC_DTL_TBL)}
         |WHERE ACTIVE=1
       """.stripMargin)

    val reltelDFAll = relClaimDFAll.
      drop("Coverage").
      drop("REL_CMP_COVERAGE").
      union(telDFAll)

    val telrelClaimDF = Util.filterEnginesByType(cfg, reltelDFAll, getEngType, esnColName = "ESN")
    val engineBuildDf = load(table = s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}",
      "ENGINE_DETAILS_ESN",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_ENGINE_NAME_DESC",
      "REL_ENGINE_PLANT",
      "REL_USER_APPL_DESC",
      "REL_BUILD_DATE").
      withColumnRenamed("ENGINE_DETAILS_ESN", "ESN").
      withColumn("REL_MONTH_BUILD_DATE", concat(substring($"REL_BUILD_DATE", 1, 4), substring($"REL_BUILD_DATE", 6, 2))).
      withColumn("REL_YEAR_BUILD_DATE", substring($"REL_BUILD_DATE", 1, 4)).
      withColumn("REL_QUARTER_BUILD_DATE", concat(year($"REL_BUILD_DATE"), lit("-Q"), quarter($"REL_BUILD_DATE"))).
      withColumn("REL_ENGINE_NAME_DESC",concat_ws("_",$"REL_ENGINE_NAME_DESC",$"REL_ENGINE_PLANT"))
    //B平台的telmatics和reliability 故障问题=》telrelclaimDF  与engine detail挂起来
    val inc_nrmDF = telrelClaimDF.join(engineBuildDf, Seq("ESN"))

    val full_time_DF0 = incident_DF.
      select("CALC_DATE").
      distinct().
      withColumn("CALC_DATE_BEGIN", date_sub($"CALC_DATE", 6)).
      coalesce(1)

    val popl_DF = inc_nrmDF.crossJoin(full_time_DF0).
      filter(full_time_DF0("CALC_DATE") >= inc_nrmDF("REL_FAILURE_DATE") &&
        full_time_DF0("CALC_DATE_BEGIN") <= inc_nrmDF("REL_FAILURE_DATE")).
      select(
        "CALC_DATE",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_MONTH_BUILD_DATE",
        "REL_ENGINE_NAME_DESC",
        "CODE",
        "REL_QUARTER_BUILD_DATE",
        "REL_USER_APPL_DESC",
        //"REL_CMP_ENGINE_NAME",
        "REL_YEAR_BUILD_DATE",
        "TOTAL_COST")

    //相同的故障在某一计算周期中的 总花费
    val popl_net_amout_df = popl_DF.groupBy(
      "CODE",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_YEAR_BUILD_DATE",
      "REL_QUARTER_BUILD_DATE",
      "REL_MONTH_BUILD_DATE",
      "REL_ENGINE_NAME_DESC",
      "REL_USER_APPL_DESC",
      //"REL_CMP_ENGINE_NAME",
      //"SOURCE",
      "CALC_DATE"
    ).
      agg(sum("Total_Cost").alias("REL_CMP_SUM_NET_AMOUNT"))

    /**
      * 合并COUNT_NBR、NET_AMOUNT，计算All
      */
    val count_nbr_and_net_amount_df = inc_count_nbr_df.
      join(popl_net_amout_df,
        Seq("CODE",
          "REL_OEM_NORMALIZED_GROUP",
          "REL_YEAR_BUILD_DATE",
          "REL_QUARTER_BUILD_DATE",
          "REL_MONTH_BUILD_DATE",
          "REL_ENGINE_NAME_DESC",
          "REL_USER_APPL_DESC",
          //"REL_CMP_ENGINE_NAME",
          //"SOURCE",
          "CALC_DATE"),
        "left_outer").coalesce(20).persist(StorageLevel.MEMORY_AND_DISK)

    val incCombCols: Seq[String] = Seq("REL_USER_APPL_DESC", "REL_OEM_NORMALIZED_GROUP", "REL_YEAR_BUILD_DATE", "REL_QUARTER_BUILD_DATE", "REL_MONTH_BUILD_DATE")

    val df_with_all2 = Util.getAllPossibleDataframe(count_nbr_and_net_amount_df, count_nbr_and_net_amount_df.columns, incCombCols)

    val df_with_all1 = spark.createDataFrame(df_with_all2.rdd, df_with_all2.schema).persist(StorageLevel.MEMORY_AND_DISK)

    val df_with_all0 = df_with_all1.filter(
      """   (REL_YEAR_BUILD_DATE == 'All' and REL_QUARTER_BUILD_DATE == 'All' AND REL_MONTH_BUILD_DATE == 'All')
        |OR (REL_YEAR_BUILD_DATE != 'All' and REL_QUARTER_BUILD_DATE == 'All' AND REL_MONTH_BUILD_DATE == 'All')
        |OR (REL_YEAR_BUILD_DATE != 'All' and REL_QUARTER_BUILD_DATE != 'All')
      """.stripMargin)

    val df_with_all = df_with_all0.groupBy(
      "CODE",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_YEAR_BUILD_DATE",
      "REL_QUARTER_BUILD_DATE",
      "REL_MONTH_BUILD_DATE",
      "REL_ENGINE_NAME_DESC",
      "REL_USER_APPL_DESC",
      "REL_CMP_ENGINE_NAME",
      "SOURCE",
      "CALC_DATE").
      agg(sum("COUNT_NBR").as("COUNT_NBR"),
        sum("REL_CMP_SUM_NET_AMOUNT").alias("REL_CMP_SUM_NET_AMOUNT"))

    /**
      * 构造输出结果
      */
    val full_time_DF = df_with_all.select("CALC_DATE").distinct()

    val full_comb_df = df_with_all.select(
      "CODE",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_YEAR_BUILD_DATE",
      "REL_QUARTER_BUILD_DATE",
      "REL_MONTH_BUILD_DATE",
      "REL_ENGINE_NAME_DESC",
      "REL_USER_APPL_DESC",
      "REL_CMP_ENGINE_NAME",
      "SOURCE").
      distinct()

    // coalesce the full_time_DF to one partition (sparsely distributed to 400 partitions by default)
    // careful with cross join - default partitions of 400 ^ 2 = 160000 partitions
    val full_comb_time_DF0 = full_comb_df.crossJoin(full_time_DF.coalesce(1))
    val full_comb_time_DF = spark.createDataFrame(full_comb_time_DF0.rdd, full_comb_time_DF0.schema).persist(StorageLevel.MEMORY_AND_DISK)

    val lft_join_DF = full_comb_time_DF.
      join(df_with_all,
        Seq("CODE",
          "REL_OEM_NORMALIZED_GROUP",
          "REL_YEAR_BUILD_DATE",
          "REL_QUARTER_BUILD_DATE",
          "REL_MONTH_BUILD_DATE",
          "REL_ENGINE_NAME_DESC",
          "REL_USER_APPL_DESC",
          "REL_CMP_ENGINE_NAME",
          "SOURCE",
          "CALC_DATE"),
        "left_outer").
      withColumnRenamed("REL_YEAR_BUILD_DATE", "BUILD_YEAR")

    val final_output0 = lft_join_DF.withColumn("REL_MONTH_BUILD_DATE", when($"REL_MONTH_BUILD_DATE" =!= "All", concat(substring($"REL_MONTH_BUILD_DATE", 1, 4), lit("-"), substring($"REL_MONTH_BUILD_DATE", 5, 2))).otherwise(lit("All"))).
      filter($"CODE".isNotNull && $"CODE".toString.length > 0).
      withColumn("REL_CMP_SUM_NET_AMOUNT", col("REL_CMP_SUM_NET_AMOUNT").cast("integer")).
      withColumn("COUNT_NBR", when($"COUNT_NBR".isNull || $"CODE" === "", lit(0)).otherwise($"COUNT_NBR")).
      withColumn("CODE", when($"CODE".isNull || $"CODE" === "", lit("NONE")).otherwise($"CODE"))
    val final_output = final_output0.
      select("CODE",
        "CALC_DATE",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_MONTH_BUILD_DATE",
        "REL_ENGINE_NAME_DESC",
        "SOURCE",
        "REL_QUARTER_BUILD_DATE",
        "BUILD_YEAR",
        "REL_USER_APPL_DESC",
        "REL_CMP_ENGINE_NAME",
        "COUNT_NBR",
        "REL_CMP_SUM_NET_AMOUNT")
      .where($"REL_MONTH_BUILD_DATE" === "All")

    println("RDD partition size: " + final_output.rdd.partitions.size)

    final_output
  }
//  def getResult(result:Dataset[Row]): Dataset[Row] ={
//    if(cfg(Constants.DATA_SAMPLE).equalsIgnoreCase("true")){
//      logger.warn(s"""WARN :: Saving data WITHOUT 'All' >>>>>>>>>>>>>>>>>>>>>>>>>>>""")
//      result
//        .where("CODE != 'All' and REL_USER_APPL_DESC != 'All' and REL_OEM_NORMALIZED_GROUP  != 'All' and REL_YEAR_BUILD_DATE != 'All' and REL_QUARTER_BUILD_DATE != 'All' and REL_MONTH_BUILD_DATE != 'All'")
//    } else
//      result
//  }
}
