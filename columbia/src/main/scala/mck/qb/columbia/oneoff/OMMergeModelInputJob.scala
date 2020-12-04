package mck.qb.columbia.oneoff

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.Job
import org.apache.spark.sql.DataFrame

object OMMergeModelInputJob extends Job with MergeModelInputBaseJob {

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_DETECT_OM)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  override def main(args: Array[String]): Unit = {
    checkArgs(args)
    super.main(args)
  }

  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {

    val engTypes: Seq[String] = Seq("b", "d", "f", "l", "m", "ng", "x", "z")
    saveAsSingleFile(engTypes)
  }

  override def saveAsSingleFile(engTypes: Seq[String]): Unit = {
    engTypes.foreach((egnType: String) => {
      //Warranty/Policy/Campaign_TRP/Field_test/Emission/All
      Seq(
        "om_build_volume_" + egnType,
        "occurrence_mntr_warranty_" + egnType,
        "occurrence_mntr_policy_" + egnType,
        "occurrence_mntr_campaign_trp_" + egnType,
        "occurrence_mntr_field_test_" + egnType,
        "occurrence_mntr_emission_" + egnType,
        "occurrence_mntr_all_" + egnType,
        "occurrence_mntr_tel_all_" + egnType
      ).foreach((tbl: String) => {
        logger.warn(s"""READING>>>>>>  ${cfg(Constants.MODEL_IP_DB)}.$tbl""")
        val df: DataFrame = spark.table(s"""${cfg(Constants.MODEL_IP_DB)}.$tbl""").coalesce(1)
        saveFile(df, tbl, egnType, IM_PATH)
      })
    })
  }

  override def getSavePath(tbl: String, warehousePath: String, format: String, egnType: String): String = {
    val savePath: String = warehousePath + "/" + "mi" + "/" + format + "/" + egnType + "/" + tbl.substring(0, tbl.lastIndexOf('_'))
    savePath
  }
}
