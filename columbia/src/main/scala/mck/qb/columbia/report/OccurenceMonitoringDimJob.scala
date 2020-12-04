package mck.qb.columbia.report

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.{Column, DataFrame}


object OccurenceMonitoringDimJob extends Job with Transport {


  case class Writer(df: DataFrame, tbl: String)


  val myConf: Config = AppConfig.getMyConfig(Constants.RPT_RPT_OM_RPT)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  override def run(): Unit = {


    val om_output_distinct_df = spark.sql(
      s"""SELECT DISTINCT SUBSTRING(REL_ENGINE_NAME_DESC,1,INSTR(REL_ENGINE_NAME_DESC,"_")-1) AS ENGINE_PLATFORM,
        |REL_ENGINE_NAME_DESC,
        |REL_ENGINE_PLANT,
        |ENGINE_SERIES,
        |BUILD_YEAR,
        |PROGRAM_GROUP_NAME FROM ${cfg(Constants.REPORT_DB)}.${cfg(Constants.OM_OUTPUT_TBL)}""".stripMargin)
    Util.saveData(om_output_distinct_df, cfg(Constants.REPORT_DB), "DIM_OM_OUTPUT_DISTINCT", cfg(Constants.HDFS_PATH))

    val om_stats_distinct_df = spark.sql(
      s"""SELECT DISTINCT SUBSTRING(REL_ENGINE_NAME_DESC,1,INSTR(REL_ENGINE_NAME_DESC,"_")-1) AS ENGINE_PLATFORM,
        |REL_OEM_NORMALIZED_GROUP,
        |REL_ENGINE_NAME_DESC,
        |BUILD_YEAR FROM ${cfg(Constants.REPORT_DB)}.${cfg(Constants.OM_STATS_TBL)}""".stripMargin)
    Util.saveData(om_stats_distinct_df, cfg(Constants.REPORT_DB), "DIM_OM_STATS_DISTINCT", cfg(Constants.HDFS_PATH))

    val calc_id_info_df = spark.sql(
      s"""select * from (
         |select distinct CALC_ID,
         |CODE,
         |REL_USER_APPL_DESC,
         |REL_ENGINE_NAME_DESC,
         |REL_OEM_NORMALIZED_GROUP,
         |REL_MONTH_BUILD_DATE,
         |REL_QUARTER_BUILD_DATE,
         |BUILD_YEAR,
         |SOURCE,
         |ROW_NUMBER() over (partition by CALC_ID order by CODE) as seq
         |from ${cfg(Constants.REPORT_DB)}.${cfg(Constants.OM_STATS_TBL)} WHERE CALC_ID!="#NAME?"
         |) as temp where seq=1""".stripMargin)
    Util.saveData(calc_id_info_df, cfg(Constants.REPORT_DB), "DIM_CALC_ID", cfg(Constants.HDFS_PATH))

    }
}
