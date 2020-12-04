package mck.qb.columbia.oneoff

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.columbia.oneoff.TelHeartbeat.{cfg, spark}
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame

/**
  * xxx
  * creat by newforesee 2019-09-20
  */
object CreateRefEsn extends Job{

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_UV_MST)

   def cfg: String => String = myConf.getString
  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {
    val day: DataFrame = spark.sql(
      s"""
         |select
         | engine_serial_number,
         | occurrence_date_time as first_connection_date
         | from qa_dev_raw.v_tel_hb_partition_day
         | where 0=1
          """.stripMargin)
    Util.saveData(day,"qa_dev_reference","REF_ESN_FIRST_CONNECTION_DATE",cfg(Constants.HDFS_PATH))
  }
}
