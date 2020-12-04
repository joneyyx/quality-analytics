package mck.qb.columbia.oneoff

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.Job

/**
  * Created by newforesee on 2020/4/15
  */
object WeibullMergeModelInputJob extends Job with Transport with MergeModelInputBaseJob {

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_WB_FET)
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
    val tables = Seq(
      s"${cfg(Constants.MIP_WB_CLM_TBL)}",
      s"${cfg(Constants.MIP_WB_ENG_TBL)}"
    )
    saveAsSingleFile(tables)

  }

  override def getSavePath(tbl: String, warehousePath: String, format: String, egnType: String): String = {
    val savePath: String = warehousePath + "/" + "mi" + "/" + "weibull" + "/" + tbl
    savePath
  }

  override def saveAsSingleFile(tables: Seq[String]): Unit = {
    tables.foreach(tbl => {
      val df = spark.table(s"${cfg(Constants.MODEL_IP_DB)}.$tbl").coalesce(1)
      saveFile(df, tbl, "", IM_PATH)
    })
  }
}
