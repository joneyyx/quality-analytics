package mck.qb.columbia.report

import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.WrappedArray

object UniversalViewRepCodeRelevanceJob extends Job with Transport{
  import spark.implicits._

  case class Writer(df: DataFrame, tbl: String)

  val myConf = AppConfig.getMyConfig(Constants.RPT_RPT_UV_RVC)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {

    val tblDfMap: Seq[String] = Seq("B", "D", "F", "L", "M", "NG", "X", "Z")
    val dmUnionDF = tblDfMap.map(detectMaster(_)).reduce(_.union(_))
      .coalesce(400)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val dfSeq = Seq(
      Writer(buildFaultCodeRelevance(dmUnionDF),cfg(Constants.UV_FAU_CODE_REL_TBL)), //UV_FAULT_CODE_RELEVANCE
      Writer(buildFailurePartRelevance(dmUnionDF),cfg(Constants.UV_FAI_PAR_REL_TBL))  //UV_FAILURE_PART_RELEVANCE
    )

    dfSeq.foreach((writer: Writer) => {
      logger.warn("INFO :: Saving >>>>>> [%s] As [%s]".format(writer.df, writer.tbl))
      writer match {
        case Writer(df, tbl) => Util.saveData(df, cfg(Constants.REPORT_DB), tbl, cfg(Constants.HDFS_PATH))
      }
    })
  }

  def detectMaster(engineType: String): DataFrame = {
    load(
      s"""
         |SELECT REL_ESN,
         |REL_USER_APPL_DESC,
         |REL_ENGINE_PLATFORM,
         |REL_ENGINE_NAME_DESC,
         |REL_OEM_NORMALIZED_GROUP,
         |REL_CMP_COVERAGE,
         |REL_BUILD_DATE,
         |FAULT_CODE_LIST,
         |NONIM_FAULT_CODE_LIST,
         |REL_FAILURE_DATE,
         |TEL_FAILURE_DATE,
         |REL_CMP_FAILURE_PART_LIST,
         |FAULT_CODE_DESCRIPTION_LIST,
         |NONIM_FAULT_CODE_DESCRIPTION_LIST,
         |FAULT_CODE_REASON_LIST,
         |NONIM_FAULT_CODE_REASON_LIST
         |FROM ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.FULL_FET_TBL)}_${engineType}
      """.stripMargin).
      withColumn("BUILD_QUARTER", concat(substring($"REL_BUILD_DATE", 1, 4), lit("-Q"), quarter($"REL_BUILD_DATE"))).
      withColumn("PLATFORM", lit(engineType))
  }

  def buildFaultCodeRelevance(dmUnionDF: DataFrame):DataFrame={

    val dmUnionfilterDF = dmUnionDF
      .select(
        "REL_ESN",
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_CMP_COVERAGE",
        "REL_BUILD_DATE",
        "TEL_FAILURE_DATE",
        "FAULT_CODE_LIST",
        "NONIM_FAULT_CODE_LIST" ,
        "FAULT_CODE_DESCRIPTION_LIST",
        "NONIM_FAULT_CODE_DESCRIPTION_LIST",
        "FAULT_CODE_REASON_LIST",
        "NONIM_FAULT_CODE_REASON_LIST"
      )
      .withColumn("REL_BUILD_MONTH",concat(substring($"REL_BUILD_DATE",1,4),substring($"REL_BUILD_DATE",6,2)))
      .withColumn("Union_FAULT_CODE_LIST",pickUp($"FAULT_CODE_LIST",$"NONIM_FAULT_CODE_LIST"))
      .withColumn("REV_FAULT_CODE_DESCRIPTION_LIST",pickUp($"FAULT_CODE_DESCRIPTION_LIST",$"NONIM_FAULT_CODE_DESCRIPTION_LIST"))
      .withColumn("REV_FAULT_CODE_REASON_LIST",pickUp($"FAULT_CODE_REASON_LIST",$"NONIM_FAULT_CODE_REASON_LIST"))
      .drop("FAULT_CODE_LIST","FAULT_CODE_DESCRIPTION_LIST","NONIM_FAULT_CODE_DESCRIPTION_LIST")
      .drop("NONIM_FAULT_CODE_LIST","FAULT_CODE_REASON_LIST","NONIM_FAULT_CODE_REASON_LIST")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val  faultcodeDF = spark.table(s"${cfg(Constants.REPORT_DB)}.${cfg(Constants.UV_CODE_DESCRIPTION_TBL)}")
      .select("code")
      .distinct()
      .coalesce(10)

    val faultcodecountDF = dmUnionfilterDF
      .join(faultcodeDF,compare(dmUnionfilterDF("Union_FAULT_CODE_LIST"),faultcodeDF("code")))
      .withColumn("newCols",ZipFaultCodeWithLOC($"Union_FAULT_CODE_LIST",$"REV_FAULT_CODE_DESCRIPTION_LIST",$"REV_FAULT_CODE_REASON_LIST"))
      .withColumn("newCol",explode($"newCols"))
      .withColumn("REV_FAULT_CODE",$"newCol" ("_1"))
      .withColumn("REV_FAULT_CODE_DESCRIPTION",$"newCol" ("_2"))
      .withColumn("REV_FAULT_CODE_REASON",$"newCol" ("_3"))
      .withColumnRenamed("code","REF_FAULT_CODE")
      .where($"REF_FAULT_CODE"=!=$"REV_FAULT_CODE")
      .drop("Union_FAULT_CODE_LIST")
      .select(
        "REL_ESN",
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_CMP_COVERAGE",
        "REL_BUILD_MONTH",
        "TEL_FAILURE_DATE",
        "REF_FAULT_CODE",
        "REV_FAULT_CODE",
        "REV_FAULT_CODE_DESCRIPTION",
        "REV_FAULT_CODE_REASON"
      ).distinct()
      .groupBy(
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_CMP_COVERAGE",
        "REL_BUILD_MONTH",
        "REF_FAULT_CODE",
        "REV_FAULT_CODE",
        "REV_FAULT_CODE_DESCRIPTION",
        "REV_FAULT_CODE_REASON"
      ).count()
      .withColumnRenamed("count","REV_FAULT_CODE_COUNT")

    faultcodecountDF

  }

  def buildFailurePartRelevance(dmUnionDF: DataFrame):DataFrame={
    val dmUnionfilterDF = dmUnionDF
      .select(
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_CMP_COVERAGE",
        "REL_BUILD_DATE",
        "FAULT_CODE_LIST",
        "REL_FAILURE_DATE",
        "REL_ESN",
        "NONIM_FAULT_CODE_LIST",
        "REL_CMP_FAILURE_PART_LIST",
        "TEL_FAILURE_DATE",
        "FAULT_CODE_DESCRIPTION_LIST",
        "NONIM_FAULT_CODE_DESCRIPTION_LIST",
        "FAULT_CODE_REASON_LIST",
        "NONIM_FAULT_CODE_REASON_LIST"
      )
      .withColumn("REL_BUILD_MONTH",concat(substring($"REL_BUILD_DATE",1,4),substring($"REL_BUILD_DATE",6,2)))
      .withColumn("Union_FAULT_CODE_LIST",pickUp($"FAULT_CODE_LIST",$"NONIM_FAULT_CODE_LIST"))
      .withColumn("REV_FAULT_CODE_DESCRIPTION_LIST",pickUp($"FAULT_CODE_DESCRIPTION_LIST",$"NONIM_FAULT_CODE_DESCRIPTION_LIST"))
      .withColumn("REV_FAULT_CODE_REASON_LIST",pickUp($"FAULT_CODE_REASON_LIST",$"NONIM_FAULT_CODE_REASON_LIST"))
      .drop("FAULT_CODE_LIST","FAULT_CODE_DESCRIPTION_LIST","NONIM_FAULT_CODE_DESCRIPTION_LIST")
      .drop("NONIM_FAULT_CODE_LIST","FAULT_CODE_REASON_LIST","NONIM_FAULT_CODE_REASON_LIST")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val failurePartLeft = dmUnionfilterDF.
      select(
        "REL_USER_APPL_DESC",
        "REL_ENGINE_PLATFORM",
        "REL_ENGINE_NAME_DESC",
        "REL_OEM_NORMALIZED_GROUP",
        "REL_CMP_COVERAGE",
        "REL_BUILD_DATE",
        "REL_FAILURE_DATE",
        "REL_ESN",
        "REL_CMP_FAILURE_PART_LIST",
        "REL_BUILD_MONTH"
      )
      .withColumn("REF_FAILURE_PART", explode($"REL_CMP_FAILURE_PART_LIST"))
      .drop("REL_CMP_FAILURE_PART_LIST")

    val faultCodeRight = dmUnionfilterDF.
      select(
        "REL_ESN",
        "TEL_FAILURE_DATE",
        "Union_FAULT_CODE_LIST",
        "REV_FAULT_CODE_DESCRIPTION_LIST",
        "REV_FAULT_CODE_REASON_LIST"
      ).withColumnRenamed("REL_ESN", "ESN")
      .filter("TEL_FAILURE_DATE is not null")

    val joined = failurePartLeft.join(faultCodeRight,
        failurePartLeft("REL_ESN") === faultCodeRight("ESN") &&
        failurePartLeft("REL_FAILURE_DATE") <= date_add(faultCodeRight("TEL_FAILURE_DATE"), 15) &&
        failurePartLeft("REL_FAILURE_DATE") >= faultCodeRight("TEL_FAILURE_DATE"),
      "left_outer"
    )
      .filter("Union_FAULT_CODE_LIST is not null")
      .withColumn("newCols",ZipFaultCodeWithLOC($"Union_FAULT_CODE_LIST",$"REV_FAULT_CODE_DESCRIPTION_LIST",$"REV_FAULT_CODE_REASON_LIST"))
      .withColumn("newCol",explode($"newCols"))
      .withColumn("REV_FAULT_CODE",$"newCol" ("_1"))
      .withColumn("REV_FAULT_CODE_DESCRIPTION",$"newCol" ("_2"))
      .withColumn("REV_FAULT_CODE_REASON",$"newCol" ("_3"))
      .drop("newCol","Union_FAULT_CODE_LIST", "ESN")
      .distinct()

    val revCount = joined.groupBy(
      "REL_USER_APPL_DESC",
      "REL_ENGINE_PLATFORM",
      "REL_ENGINE_NAME_DESC",
      "REL_OEM_NORMALIZED_GROUP",
      "REL_CMP_COVERAGE",
      "REL_BUILD_MONTH",
      "REF_FAILURE_PART",
      "REV_FAULT_CODE",
      "REV_FAULT_CODE_DESCRIPTION",
      "REV_FAULT_CODE_REASON"
    ).count()
      .withColumnRenamed("count", "REV_FAULT_CODE_COUNT")
    revCount
  }

  def compare = udf(
    (code_list: WrappedArray[String], code: String) => {
      var flag = false
      for (elem <- code_list) {
        if (elem.equals(code)) flag=true
      }
      flag
    })

  def pickUp = udf(
    (a:mutable.WrappedArray[String],b:mutable.WrappedArray[String])=>{
      a.toList ::: b.toList
    }
  )

  def ZipFaultCodeWithLOC: UserDefinedFunction = udf {
    (fault_code_list: Seq[String], latitude_list: Seq[String], longitude_list: Seq[String]) => {
      val values = fault_code_list.zip(latitude_list).zip(longitude_list)
      values.map(value => (value._1._1, value._1._2,value._2))
    }
  }

}

