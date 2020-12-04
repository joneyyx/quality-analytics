package mck.qb.columbia.report

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Build the permission rule table
  * creat by newforesee 2019-08-13
  */
object AuthoriyRolesDimBuildJob extends Job {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.RPT_RPT_AR_DIM)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  override def run(): Unit = {
    //Load the data and union
    val res_date: DataFrame = load_data

    //Loading comparison table[FAILURE_PART,ENGINE_NAME,ROLE]
    val ref_roles: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_ROLES_TBL)}")

    //Building dimension tables
    val roles_dim: Dataset[Row] = build_result(res_date, ref_roles)

    //Save the result table
    Util.saveData(roles_dim, cfg(Constants.REPORT_DB), cfg(Constants.DIM_PERMISSION_TBL), cfg(Constants.HDFS_PATH))
  }

  /**
    * Load the data from the data table and collate it to get the important columns
    * @return
    */
  def load_data: Dataset[Row] = {

    def load(db_name: String, tbl: String, code: String = "CODE", engine_name: String = "ENGINE_NAME"): DataFrame = {
      spark.table(s"$db_name.$tbl")
        .select(s"$code", s"$engine_name")
        .withColumnRenamed(s"$code", "CODE")
        .withColumnRenamed(s"$engine_name", "ENGINE_NAME")
        .distinct()
        .withColumn("FAILURE_PARTS", Util.splitString2Set($"CODE"))
        .withColumn("FAILURE_PART", explode($"FAILURE_PARTS"))
    }

    load(s"${cfg(Constants.REPORT_DB)}", s"${cfg(Constants.UV_TC_DATE_TBL)}")
      .union(load(s"${cfg(Constants.REPORT_DB)}", s"${cfg(Constants.OM_OUTPUT_TBL)}"))
      .union(load(s"${cfg(Constants.REPORT_DB)}", s"${cfg(Constants.OM_STATS_TBL)}"))
      .union(load(s"${cfg(Constants.REPORT_DB)}", s"${cfg(Constants.UV_MASTER_TBL)}", "REL_CMP_FAILURE_PART_LIST"))

  }

  /**
    * Extract methods just to keep the code clean
    * @param res_date
    * @param ref_roles
    * @return
    */
  private def build_result(res_date: DataFrame, ref_roles: DataFrame): Dataset[Row] = {
    res_date.join(ref_roles, Seq("FAILURE_PART", "ENGINE_NAME"), "left")
      .select("ENGINE_NAME","CODE", "ROLE")
      .filter($"ROLE" isNotNull)
      .withColumnRenamed("ROLE","ROLES")
      .withColumnRenamed("CODE", "FAILURE_PART")
      .distinct()
  }


}
