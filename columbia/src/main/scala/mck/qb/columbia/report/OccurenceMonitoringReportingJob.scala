package mck.qb.columbia.report

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._


object OccurenceMonitoringReportingJob extends Job with Transport {


  case class Writer(df: DataFrame, tbl: String)

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.RPT_RPT_OM_RPT)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  override def run(): Unit = {


    val reference_df: DataFrame = builReferenceDF.coalesce(1)
    val om_output_df: DataFrame = getOmOutputDF

    val om_stats_df: DataFrame = getOmStatsDF
      //.withColumn("REL_QUARTER_BUILD_DATE",regexp_replace($"REL_QUARTER_BUILD_DATE","/","-"))
      //.withColumn("REL_QUARTER_BUILD_DATE",concat(substring($"REL_QUARTER_BUILD_DATE", 1, 4), lit("-Q"), quarter($"REL_QUARTER_BUILD_DATE")))

    val output_df: DataFrame = buildResultDF(om_output_df,reference_df,addHilight = true)
    Util.saveData(output_df, cfg(Constants.REPORT_DB), cfg(Constants.OM_OUTPUT_TBL), cfg(Constants.HDFS_PATH))

    val stats_df: DataFrame = buildResultDF(om_stats_df,reference_df)

    Util.saveData(stats_df, cfg(Constants.REPORT_DB), cfg(Constants.OM_STATS_TBL), cfg(Constants.HDFS_PATH))

  }

  private def buildResultDF(original_df: DataFrame, reference_df: DataFrame, addHilight: Boolean=false): DataFrame = {
    val split_entity_df = original_df
      .withColumn("REL_ENGINE_PLANT",substring_index($"REL_ENGINE_NAME_DESC","_",-1))
      .withColumn("REL_ENGINE_NAME_DESC",substring_index($"REL_ENGINE_NAME_DESC","_",2))
    val joinCondition: Column = split_entity_df("CODE") === reference_df("REF_CODE") && split_entity_df("REL_ENGINE_NAME_DESC") === reference_df("REF_ENGINE_NAME_DESC")
    val df_with_desc: DataFrame = split_entity_df.join(reference_df, joinCondition, "leftouter")
      .drop("REF_CODE")
      .drop("REF_ENGINE_NAME_DESC")
      .withColumn("ENGINE_NAME", Util.subStringWithSpec($"REL_ENGINE_NAME_DESC", lit("_")))
    if (addHilight) {
      val ref_hl_code: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_HL_CODE_TBL)}")
      df_with_desc.join(ref_hl_code,split_entity_df("CODE") === ref_hl_code("Fault_Code"),"left_outer")
        .drop("Fault_Code")
        .withColumn("Highlight",when($"Highlight"isNull,"0") otherwise lit(1) )
        .withColumnRenamed("Highlight","is_Highlight")
    }else df_with_desc


  }

  private def builReferenceDF: DataFrame = {
    val ref_fail_part_code_df: DataFrame = spark.table(s"""${cfg(Constants.REF_DB)}.${cfg(Constants.REF_FP_DESC_TBL)}""")
    val ref_telfc_desc_df: DataFrame = spark.table(s"""${cfg(Constants.REF_DB)}.${cfg(Constants.REF_TFC_DESC_TBL)}""")
    ref_fail_part_code_df
      .withColumn("REASON",lit("NULL"))
      .union(ref_telfc_desc_df).
      withColumnRenamed("CODE", "REF_CODE").
      withColumnRenamed("REL_ENGINE_NAME_DESC", "REF_ENGINE_NAME_DESC")
  }

  private def getOmStatsDF: DataFrame = {
    //    val om_stats_schema: Map[String, DataType] = Map(
    //
    //    )
    val om_stats_df: DataFrame = spark.table(s"""${cfg(Constants.MODEL_OP_DB)}.${cfg(Constants.OM_STATS_TBL)}""")
      //.where($"CODE" =!= "CODE")
    om_stats_df
    //    Util.applySchema(om_stats_df, om_stats_schema)
  }

  private def getOmOutputDF: DataFrame = {
    //    val om_output_Schema: Map[String, DataType] = Map(
    //
    //    )

    val om_output_df: DataFrame = spark.table(s"""${cfg(Constants.MODEL_OP_DB)}.${cfg(Constants.OM_OUTPUT_TBL)}""")
      //.where($"CALC_ID" =!= "CALC_ID")

    om_output_df
    //    Util.applySchema(om_output_df, om_output_Schema)
  }
}
