package mck.qb.columbia.report

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
  * Created by newforesee on 2020/4/20
  */
object WeiBullReportingJob extends Job with Transport {

  val myConf: Config = AppConfig.getMyConfig(Constants.RPT_RPT_WB)
  logger.warn("Config utilized - (%s)".format(myConf))
  override def cfg: String => String = myConf.getString
  import spark.implicits._

  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {
    val wb_op = spark.table(s"${cfg(Constants.MODEL_OP_DB)}.${cfg(Constants.MOP_WB_TBL)}")
      .withColumn("Entity", getEntity($"REL_ENGINE_NAME_DESC"))
      .withColumn("REL_ENGINE_NAME_DESC", getEngineName($"REL_ENGINE_NAME_DESC"))

    Util.saveData(wb_op,cfg(Constants.REPORT_DB),cfg(Constants.WB_RPT_TBL),cfg(Constants.HDFS_PATH))

  }

  def getEntity: UserDefinedFunction = udf {
    (str: String) => {
      str.substring(str.lastIndexOf("_")+1)
    }
  }

  def getEngineName: UserDefinedFunction = udf {
    (str: String) => {
      str.substring(0,str.lastIndexOf("_"))
    }
  }
}
