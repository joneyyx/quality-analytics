package mck.qb.columbia.oneoff

import mck.qb.columbia.oneoff.OMMergeModelInputJob.logger
import org.apache.spark.sql.DataFrame

/**
  * Created by newforesee on 2020/4/15
  */
trait MergeModelInputBaseJob {

  //val sourcePath: String = cfg(Constants.HDFS_PATH)
  var IM_PATH: String = _

  /**
    *
    * @param args
    */
  def checkArgs(args: Array[String]) = {
    if (args.nonEmpty && !args(0).equalsIgnoreCase("")) {
      val argsSplit: Array[String] = args(0).split("=")
      if (argsSplit(0).equalsIgnoreCase("impath")) {
        val path: String = argsSplit(1).trim
        IM_PATH = if (path.endsWith("/")) path else path + "/"
      }
    }
  }

  def saveFile(df: DataFrame, tbl: String, egnType: String, warehousePath: String, format: String = "parquet", mode: String = "overwrite") = {

    val savePath: String = getSavePath(tbl, warehousePath, format, egnType)

    format match {
      case "csv" =>
        logger.warn(s"""SAVE TO $savePath >>>>>""")
        df.write.format("csv")
          .option("delimiter", ",")
          .option("header", value = true)
          .option("compression", "none")
          .mode(mode).save(savePath)
      case _ =>
        logger.warn(s"""SAVE TO $savePath >>>>>""")
        df.write.format("parquet")
          .mode(mode).save(savePath)
    }
  }

  def getSavePath(tbl: String, warehousePath: String, format: String, egnType: String = ""): String


  def saveAsSingleFile(tables: Seq[String]): Unit
}
