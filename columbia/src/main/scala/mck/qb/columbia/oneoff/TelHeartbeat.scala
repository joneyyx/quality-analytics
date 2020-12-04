package mck.qb.columbia.oneoff

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Date

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.{Transport, withShufflePartition}
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Test Heartbeat data
  * creat by newforesee 2019-09-09
  */
object TelHeartbeat extends Job with Transport {
  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_UV_MST)

  override def cfg: String => String = myConf.getString

  import spark.implicits._

  /**
    * val formatUTC = new SimpleDateFormat("yyyyMMdd")
    * val formatCST = new SimpleDateFormat("yyyy-MM-dd")
    *
    * var startDate: LocalDate = LocalDate.parse(sdate)
    * var endDate: LocalDate = LocalDate.parse(edate)
    * val writer = new PrintWriter(new File(fileName))
    *
    */


  case class Builder(startDate: String, endDate: String, tsp_key: String)

  val date_format = new SimpleDateFormat("yyyyMMdd")
  val date_format2 = new SimpleDateFormat("yyyy-MM-dd")

  override def run(): Unit = {

    val builders: Seq[Builder] = Seq(
      Builder("2018-10-29", "2019-09-17", "Cty")
      , Builder("2017-05-10", "2019-09-17", "G7")
      , Builder("2017-04-06", "2019-09-17", "Zhike")
    )
    for (tsp <- builders) {
      //      REF_ESN_FIRST_CONNECTION_DATE
      var start: LocalDate = LocalDate.parse(tsp.startDate)
      val end: LocalDate = LocalDate.parse(tsp.endDate)
      while (start.isBefore(end)) {
        var pdate: String = date_format.format(date_format2.parse(start.toString))
        logger.warn(s"Working  On  ${tsp.tsp_key}  $pdate Data")
        val ref_esn: DataFrame = spark.table("qa_dev_reference.REF_ESN_FIRST_CONNECTION_DATE")
          .withColumnRenamed("Engine_Serial_Number", "EXIST")

        val day: DataFrame = spark.sql(
          s"""
             |select
             | *
             | from qa_dev_raw.v_tel_hb_partition_day
             | where TSP_Key='${tsp.tsp_key}' and pdate='$pdate'
          """.stripMargin)
        val result: DataFrame = day.join(ref_esn, day("Engine_Serial_Number") === ref_esn("EXIST"), "left")
          .where($"EXIST" isNull)
          .select("Engine_Serial_Number", "occurrence_date_time")
          .groupBy("Engine_Serial_Number").agg(min("occurrence_date_time") as "FIRST_CONNECTION_DATE")

        Util.saveData(result, "qa_dev_reference", "REF_ESN_FIRST_CONNECTION_DATE", cfg(Constants.HDFS_PATH), "append")
        start = start.plusDays(1)
      }
    }

    //Util.saveData(first_connection, cfg(Constants.REF_DB), "ref_esn_first_connection_date", cfg(Constants.HDFS_PATH))

  }


}
