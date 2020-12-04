package mck.qb.columbia.report

import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO._
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UniversalViewDimJob extends Job with Transport {

  import spark.implicits._

  val myConf = AppConfig.getMyConfig(Constants.RPT_RPT_UV)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  case class Writer(df: DataFrame, tbl: String)

  override def run(): Unit = {
    val uv_master_distinct_df = spark.sql(
      s"""SELECT DISTINCT SUBSTRING(REL_ENGINE_NAME_DESC,1,INSTR(REL_ENGINE_NAME_DESC,"_")-1) AS ENGINE_PLATFORM,
         |REL_ENGINE_NAME_DESC,
         |rel_cmp_coverage as REL_CMP_PROGRAM_GROUP_NAME FROM ${cfg(Constants.REPORT_DB)}.${cfg(Constants.UV_MASTER_TBL)}""".stripMargin)
    Util.saveData(uv_master_distinct_df, cfg(Constants.REPORT_DB), "DIM_UV_MASTER_DISTINCT", cfg(Constants.HDFS_PATH))

    val uv_code_description_df = spark.table(s"${cfg(Constants.REPORT_DB)}.${cfg(Constants.UV_CODE_DESCRIPTION_TBL)}")
      .select("CODE","DESCRIPTION")
    val om_code_description_df1 = spark.table(s"${cfg(Constants.REPORT_DB)}.${cfg(Constants.OM_OUTPUT_TBL)}")
      .select("CODE","DESCRIPTION")
    val om_code_description_df2 = spark.table(s"${cfg(Constants.REPORT_DB)}.${cfg(Constants.OM_STATS_TBL)}")
      .select("CODE","DESCRIPTION")
    val ref_highlight_fault_code_df = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.OM_HL_FAU_CODE_TBL)}")
    uv_code_description_df
      .union(om_code_description_df1)
      .union(om_code_description_df2)
      .withColumn("CODE",Util.removeTabWrapAndTrim($"CODE"))
      .createOrReplaceTempView("code_description")
    val uv_code_description_distinct_df = spark.sql(s"""SELECT CODE,description
                 |FROM (
                 |SELECT CODE,description,
                 |ROW_NUMBER()OVER(PARTITION BY upper(CODE) ORDER BY description desc) AS SEQ
                 |from code_description
                 |) AS uv_code_description_distinct
                 |WHERE SEQ = 1""".stripMargin)

    val uv_code_des_with_hl = uv_code_description_distinct_df.
      join(ref_highlight_fault_code_df,
        uv_code_description_distinct_df("CODE")===ref_highlight_fault_code_df("Fault_Code"),
        "left_outer")
      .drop("Fault_Code")
      .withColumn("is_HighLight",when($"Highlight".isNull,0).otherwise(lit(1)))
      .drop("Highlight")


    Util.saveData(uv_code_des_with_hl, cfg(Constants.REPORT_DB), "DIM_CODE_DESCRIPTION", cfg(Constants.HDFS_PATH))


  }

}
