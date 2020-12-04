package mck.qb.columbia.reference

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

object CreateReferenceJob extends Job {

  import spark.implicits._


  val myConf: Config = AppConfig.getMyConfig(Constants.REF_DETECT_CRT)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  override def run(): Unit = {
    //load EngineFeatures
    val engineFeaturesDF: DataFrame = getEngineFeaturesDF.cache()

    //Construct REF_TELFC_DESC
    val telFaultCodeDF: DataFrame = getTelFaultCodeDF
    val telFCDescDF: DataFrame = refTelFCDescDF(engineFeaturesDF,telFaultCodeDF)

    //Construct REF_FAIL_PART_CODE
    val engineClaimDF: DataFrame = getEngineClaimDF.coalesce(8)
    val refFailPartCodeDF: DataFrame = getJoinedDF(engineFeaturesDF, engineClaimDF, 3).coalesce(8)
    val filter_FailPartCodeDF = refFailPartCodeDF
        .select(
          "CODE",
          "REL_ENGINE_NAME_DESC",
          "DESCRIPTION"
        )

    //Save to Hive table
    Util.saveData(telFCDescDF, cfg(Constants.REF_DB), cfg(Constants.REF_TFC_DESC_TBL), cfg(Constants.HDFS_PATH))
    Util.saveData(filter_FailPartCodeDF, cfg(Constants.REF_DB), cfg(Constants.REF_FP_DESC_TBL), cfg(Constants.HDFS_PATH))

  }

  def refTelFCDescDF(engineFeaturesDF:DataFrame,telFaultCodeDF:DataFrame): Dataset[Row] ={

    def getRefTelFCDescDF(filterExpr: String, aggCode: Int): DataFrame ={
      getJoinedDF(engineFeaturesDF.where(filterExpr), telFaultCodeDF, aggCode)
        .withColumn("DESCRIPTION", array($"DESCRIPTION"))
    }

    val refTelFCDescDF_NSV: DataFrame = getRefTelFCDescDF("REL_Normal_EMISSION_LEVEL == 'NSV'",1)
    val refTelFCDescDF_NSVI: DataFrame = getRefTelFCDescDF("REL_Normal_EMISSION_LEVEL == 'NSVI'", 1)


    val joinreftelfcDF: DataFrame = join_ref_telfc_desc_NSV(refTelFCDescDF_NSV)
    val joinreftelfcDF_NSVI: DataFrame = join_ref_telfc_desc_NSVI(refTelFCDescDF_NSVI)

    joinreftelfcDF.union(joinreftelfcDF_NSVI)
  }

  private def getEngineClaimDF: DataFrame = {

    val engineClaimDetailDF: DataFrame = spark.sql(
      s"""
         |SELECT
         | ESN_NEW AS ESN,
         | FAILURE_PART AS CODE,
         | FAIL_CODE_MODE AS DESCRIPTION
         | FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_CLM_DTL_TBL)}
        """.stripMargin).
      withColumn("DESCRIPTION", when($"DESCRIPTION".rlike("^[a-zA-Z]+$"), substring($"DESCRIPTION", 1, 4)).
        otherwise(lit(null)))
    engineClaimDetailDF
  }


  private def getTelFaultCodeDF: DataFrame = {
    val telFaultCodeDF: DataFrame = spark.sql(
      s"""
         |SELECT
         | ENGINE_SERIAL_NUMBER as ESN,
         | FAULT_CODE AS CODE,
         | FAULT_CODE_DESCRIPTION AS DESCRIPTION
         | FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.TEL_FC_DTL_TBL)}
      """.stripMargin)
    telFaultCodeDF
  }

  private def getEngineFeaturesDF: DataFrame = {
    val engineDetailDF: Dataset[Row] = spark.sql(
      s"""
         |SELECT
         | ESN_NEW AS ESN,
         | ENGINE_PLATFORM AS REL_ENGINE_PLATFORM,
         | EMISSION_LEVEL AS REL_EMISSION_LEVEL,
         | ENGINE_PLANT AS REL_ENGINE_PLANT,
         | Displacement AS REL_DISPLACEMENT
         | FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_DTL_TBL)}
    """.stripMargin)
      .where("Dom_Int!='Int' and Build_Date>=to_date('2014-01-01') and Build_Date<=CURRENT_DATE() and Emission_Level in ('CS3','CS4','E6','E5','E6','NS5','NS6','S5','S6')")
      .withColumnRenamed("engine_serial_num", "ESN").dropDuplicates("ESN") //need to distinct esn

    val engineFeaturesDF: DataFrame = engineDetailDF
      .withColumn("REL_Normal_EMISSION_LEVEL", Util.getNormEmission($"REL_EMISSION_LEVEL"))
      .withColumn("REL_ENGINE_NAME_DESC", concat($"REL_ENGINE_PLATFORM", lit("_"), $"REL_Normal_EMISSION_LEVEL"))
      .withColumn("REL_ENGINE_NAME", Util.getNormDesc($"REL_ENGINE_PLATFORM"))

    engineFeaturesDF
  }

  /**
    * Return the DataFrame after the join
    *
    * @param aggCode
    * Aggregate condition code(default 1)：
    * 1 => first
    * 2 => collect_list
    * 3 => collect_set
    */
  private def getJoinedDF(leftDF: DataFrame, rightDF: DataFrame, aggCode: Int = 1): DataFrame = {
    val joinCondition: Column = rightDF("ESN") === leftDF("ESN")

    def matcher(aggCode: Int) = aggCode match {
      case 1 => first("DESCRIPTION").as("DESCRIPTION")
      case 2 => collect_list("DESCRIPTION").as("DESCRIPTION")
      case 3 => collect_set("DESCRIPTION").as("DESCRIPTION")
      case _ => first("DESCRIPTION").as("DESCRIPTION")
    }

    val refFailPartCodeDF: DataFrame = rightDF.join(leftDF, joinCondition, joinType = "left_outer").
      groupBy($"Code", $"REL_ENGINE_NAME_DESC", $"REL_ENGINE_NAME",$"REL_ENGINE_PLANT",$"REL_ENGINE_PLATFORM",$"REL_Normal_EMISSION_LEVEL",$"REL_DISPLACEMENT").
      agg(matcher(aggCode))
    refFailPartCodeDF
      .withColumn("REL_ENGINE_PLATFORM",when($"REL_ENGINE_PLATFORM"==="X" ,concat($"REL_ENGINE_PLATFORM",$"REL_DISPLACEMENT".cast("int"))).otherwise($"REL_ENGINE_PLATFORM"))
      .withColumn("REL_ENGINE_PLATFORM",
        when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF2.8",lit("F2.8")).otherwise(
          when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF3.8",lit("F3.8")).otherwise(
            when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF4.5",lit("F4.5")).otherwise(
              when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISD6.7",lit("D6.7")).otherwise(
                when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISX11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")).otherwise(
                  when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X14.5",lit("Z15")).otherwise(
                    when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")
                    ).otherwise($"REL_ENGINE_PLATFORM"))
                )
              )
            )
          ))).
      withColumn("REL_ENGINE_NAME_DESC", concat($"REL_ENGINE_PLATFORM", lit("_"), $"REL_Normal_EMISSION_LEVEL"))

  }

  private def join_ref_telfc_desc_NSV(df: DataFrame): DataFrame = {
    val nsvdf: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_NSV_FAU_COD_REA_DESC_TBL)}")
      .where($"FAULT_CODE"=!="Fault_Code")
    val nsvengdf: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_NSV_ENG_TBL)}")
      .where($"ID"=!="ID")


    val nsv_eng_df: DataFrame = nsvdf
      .join(nsvengdf, nsvdf("ENGINE_SERVICE_MODEL_FK") === nsvengdf("ID"))
      .select(
        "FAULT_CODE",
        "ENGINE_SERVICE_MODEL_NAME",
        "FAULT_REASON"
      )
      .withColumn("ENGINE_SERVICE_MODEL_NAME", concat(EngineServiceModel($"ENGINE_SERVICE_MODEL_NAME"), lit("_NSV")))

    nsv_eng_df
      .join(df,
        nsv_eng_df("FAULT_CODE") === df("code") &&
          nsv_eng_df("ENGINE_SERVICE_MODEL_NAME") === df("rel_engine_name_desc"),
        "right_outer"
      )
      .withColumn("REL_ENGINE_PLATFORM",when($"REL_ENGINE_PLATFORM"==="X" ,concat($"REL_ENGINE_PLATFORM",$"REL_DISPLACEMENT".cast("int"))).otherwise($"REL_ENGINE_PLATFORM"))
      .withColumn("REL_ENGINE_PLATFORM",
      when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF2.8",lit("F2.8")).otherwise(
       when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF3.8",lit("F3.8")).otherwise(
        when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF4.5",lit("F4.5")).otherwise(
          when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISD6.7",lit("D6.7")).otherwise(
            when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISX11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")).otherwise(
              when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X14.5",lit("Z15")).otherwise(
                when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")
              ).otherwise($"REL_ENGINE_PLATFORM"))
            )
          )
        )
      ))).
      withColumn("REL_ENGINE_NAME_DESC", concat($"REL_ENGINE_PLATFORM", lit("_"), $"REL_Normal_EMISSION_LEVEL"))
      .select(
      "Code",
      "REL_ENGINE_NAME_DESC",
      "DESCRIPTION",
      "FAULT_REASON"
    )
      .withColumnRenamed("FAULT_REASON", "REASON")

  }

  private def join_ref_telfc_desc_NSVI(df: DataFrame): DataFrame = {
    val nsvidf: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_NSVI_FAU_COD_REA_DESC_TBL)}")
      .where($"FAULT_CODE"=!="Fault_Code")
    val nsviengdf: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_NSVI_ENG_TBL)}")
      .where($"ENGINE_SERVICE_MODEL_FK"=!="ENGINE_SERVICE_MODEL_FK")
    val nsvi_eng_df: DataFrame = nsvidf
      .join(nsviengdf, nsvidf("ENGINE_SERVICE_MODEL_FK") === nsviengdf("ENGINE_SERVICE_MODEL_FK"))
      .select(
        "FAULT_CODE",
        "ENGINE_SERVICE_MODEL_NAME",
        "FAULT_REASON"
      )
    //.withColumn("ENGINE_SERVICE_MODEL_NAME",concat(EngineServiceModel($"ENGINE_SERVICE_MODEL_NAME"),lit("_NSVI")))
    nsvi_eng_df
      .join(df,
        nsvi_eng_df("FAULT_CODE") === df("code") &&
          nsvi_eng_df("ENGINE_SERVICE_MODEL_NAME") === df("REL_ENGINE_NAME"),
        "right_outer"
      )
      .withColumn("REL_ENGINE_PLATFORM",when($"REL_ENGINE_PLATFORM"==="X" ,concat($"REL_ENGINE_PLATFORM",$"REL_DISPLACEMENT".cast("int"))).otherwise($"REL_ENGINE_PLATFORM"))
      .withColumn("REL_ENGINE_PLATFORM",
      when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF2.8",lit("F2.8")).otherwise(
        when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF3.8",lit("F3.8")).otherwise(
          when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF4.5",lit("F4.5")).otherwise(
            when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISD6.7",lit("D6.7")).otherwise(
              when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISX11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")).otherwise(
                when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X14.5",lit("Z15")).otherwise(
                  when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")
                ).otherwise($"REL_ENGINE_PLATFORM"))
              )
            )
          )
        )))
      .withColumn("REL_ENGINE_NAME_DESC", concat($"REL_ENGINE_PLATFORM", lit("_"), $"REL_Normal_EMISSION_LEVEL"))
      .select(
      "Code",
      "REL_ENGINE_NAME_DESC",
      "DESCRIPTION",
      "FAULT_REASON"
    )
      .withColumnRenamed("FAULT_REASON", "REASON")
  }

  def EngineServiceModel: UserDefinedFunction = udf(
    (str: String) => {
      val strings: Array[String] = str.split(" ")
      strings(0)
    }
  )
}
