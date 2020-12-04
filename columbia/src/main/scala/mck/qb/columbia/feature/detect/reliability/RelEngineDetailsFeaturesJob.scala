package mck.qb.columbia.feature.detect.reliability

import com.typesafe.config.Config
import mck.qb.library.Job
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.udf
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.Util
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RelEngineDetailsFeaturesJob extends Job {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.FET_REL_ENG_DTL)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  override def run(): Unit = {

    //Read Engine Details From PRIMARY Tables
    // Engine master list - this table contains details about all the engines made

    val engineDetailDF = spark.sql(
      s"""
         |SELECT
         |ESN_NEW AS ESN,
         |SO_NUM,
         |BUILD_DATE,
         |EMISSION_LEVEL,
         |ATS_TYPE,
         |HORSEPOWER,
         |USERAPP,
         |VEHICLE_TYPE,
         |OEM_NAME,
         |ENGINE_PLANT,
         |IN_SERVICE_DATE_AVE_TIME_LAG,
         |DOM_INT,
         |ENGINE_TYPE_DESC,
         |WAREHOUSE,
         |ACTUAL_IN_SERVICE_DATE,
         |ENGINE_PLATFORM,
         |ENGINE_GROUP_DESC,
         |ABO_QUALITY_PERFORMANCE_GROUP,
         |CCI_CHANNEL_SALES_Y_N,
         |ENGINE_MODEL,
         |Displacement
         |FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_DTL_TBL)}
    """.stripMargin)
      .where("(Dom_Int!='Int' or Dom_Int is null) and Build_Date>=to_date('2014-01-01') and Build_Date<=CURRENT_DATE() and Emission_Level in ('CS3','CS4','E6','E5','NS5','NS6','S5',' ')")
      .withColumnRenamed("engine_serial_num", "ESN").dropDuplicates("ESN") //need to distinct esn

    val engineFeatureDF = engineDetailDF.
      withColumnRenamed("ESN", "REL_ESN").
      withColumnRenamed("SO_NUM", "REL_SO_NUM").
      withColumnRenamed("Build_Date", "REL_BUILD_DATE").
      withColumnRenamed("Emission_Level", "REL_EMISSION_LEVEL").
      withColumnRenamed("ATS_Type", "REL_ATS_TYPE").
      withColumnRenamed("Horsepower", "REL_HORSEPOWER").
      withColumnRenamed("USERAPP", "REL_USER_APPL_DESC").
      withColumnRenamed("Vehicle_Type", "REL_VEHICLE_TYPE").
      withColumnRenamed("OEM_Name", "REL_OEM_NAME").
      withColumnRenamed("Engine_Plant", "REL_ENGINE_PLANT").
      withColumnRenamed("In_Service_Date_Ave_Time_Lag", "REL_IN_SERVICE_DATE_AVE_TIME_LAG").
      withColumnRenamed("Engine_Type_Desc", "REL_ENGINE_TYPE_DESC").
      withColumnRenamed("Warehouse", "REL_WAREHOUSE").
      withColumnRenamed("Actual_In_Service_Date", "REL_ACTUAL_IN_SERVICE_DATE").
      withColumnRenamed("Engine_Platform", "REL_ENGINE_PLATFORM").
      withColumnRenamed("Engine_Group_Desc", "REL_ENGINE_GROUP_DESC").
      withColumnRenamed("ABO_Quality_Performance_Group", "REL_ABO_QUALITY_PERFORMANCE_GROUP").
      withColumnRenamed("CCI_Channel_Sales_Y_N", "REL_CCI_CHANNEL_SALES_Y_N").
      withColumnRenamed("ENGINE_MODEL", "REL_ENGINE_MODEL").
      withColumnRenamed("Displacement","REL_DISPLACEMENT").
      drop("Dom_Int")

    val engineFeaturesMasterDF = engineFeatureDF.select("REL_ESN",
      "REL_SO_NUM",
      "REL_BUILD_DATE",
      "REL_EMISSION_LEVEL",
      "REL_ATS_TYPE",
      "REL_HORSEPOWER",
      "REL_USER_APPL_DESC",
      "REL_VEHICLE_TYPE",
      "REL_OEM_NAME",
      "REL_ENGINE_PLANT",
      "REL_IN_SERVICE_DATE_AVE_TIME_LAG",
      "REL_ENGINE_TYPE_DESC",
      "REL_WAREHOUSE",
      "REL_ACTUAL_IN_SERVICE_DATE",
      "REL_ENGINE_PLATFORM",
      "REL_ENGINE_GROUP_DESC",
      "REL_ABO_QUALITY_PERFORMANCE_GROUP",
      "REL_CCI_CHANNEL_SALES_Y_N",
      "REL_ENGINE_MODEL",
      "REL_DISPLACEMENT"
    ).
      withColumn("REL_BUILD_YEAR", year($"REL_BUILD_DATE")).
      withColumn("REL_BUILD_MONTH", month($"REL_BUILD_DATE")).
      withColumn("REL_BUILD_DAY_OF_MONTH", dayofmonth($"REL_BUILD_DATE")).
      withColumn("REL_BUILD_DAY_OF_WEEK", date_format($"REL_BUILD_DATE", "EEEE")).
      withColumn("REL_BUILT_ON_WEEKEND", Util.isWeekendUDF($"REL_BUILD_DAY_OF_WEEK")).
      withColumn("REL_EMISSION_LEVEL", when($"REL_ENGINE_PLATFORM"==="QSB7", lit("CS3")).otherwise($"REL_EMISSION_LEVEL")).
      withColumn("REL_Normal_EMISSION_LEVEL", Util.getNormEmission($"REL_EMISSION_LEVEL")).
      withColumn("REL_ENGINE_PLATFORM",when($"REL_ENGINE_PLATFORM"==="X" ,concat($"REL_ENGINE_PLATFORM",$"REL_DISPLACEMENT".cast("int"))).otherwise($"REL_ENGINE_PLATFORM"))
      .withColumn("REL_ENGINE_PLATFORM",
      when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF2.8",lit("F2.8")).otherwise(
        when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF3.8",lit("F3.8")).otherwise(
          when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISF4.5",lit("F4.5")).otherwise(
            when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISD6.7",lit("D6.7")).otherwise(
              when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="ISX11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12")).otherwise(
                when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X14.5",lit("Z15")).otherwise(
                  when($"REL_Normal_EMISSION_LEVEL"==="NSVI"&&$"REL_ENGINE_PLATFORM"==="X11.8"&&$"REL_ENGINE_PLANT"==="XCEC",lit("X12-XCEC")
                  ).otherwise($"REL_ENGINE_PLATFORM"))
              )
            )
          )
        ))).
      withColumn("REL_ENGINE_NAME_DESC", concat($"REL_ENGINE_PLATFORM", lit("_"), $"REL_Normal_EMISSION_LEVEL")). //将REL_ENGINE_PLATFORM 和REL_Normal_EMISSION_LEVEL合并用下划线连接).
      withColumn("REL_CMP_TIME_DIFF_BUILDDATE_TO_INSERVICEDATE_DAYS",
      datediff(engineFeatureDF("REL_ACTUAL_IN_SERVICE_DATE"), engineFeatureDF("REL_BUILD_DATE"))).
      withColumnRenamed("REL_ESN", "ENGINE_DETAILS_ESN").
      withColumn("REL_CMP_ENGINE_NAME", Util.getNormDesc($"REL_ENGINE_PLATFORM")).
      withColumn("REL_OEM_NORMALIZED_GROUP", Util.getNormOEMGrp($"REL_OEM_NAME"))
      .withColumn("REL_USER_APPL_DESC",when($"REL_USER_APPL_DESC".isNull || $"REL_USER_APPL_DESC".isin(""," " ),lit("NULL")).otherwise($"REL_USER_APPL_DESC"))
      .drop("REL_DISPLACEMENT")
      .filter($"REL_Normal_EMISSION_LEVEL" =!= "Others")

    //this function in order to add the first connection date
    val getResult: DataFrame => DataFrame = (df:DataFrame)=>{
      val wind: WindowSpec = Window.partitionBy("ESN").orderBy("FIRST_CONNECTION_TIME")
      val ref_first_connection: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_FST_CONT_TBL)}")
        .withColumn("rank",row_number().over(wind))
        .select("ESN","FIRST_CONNECTION_TIME")
        .where($"rank"===1)
          //.withColumnRenamed("ENGINE_SERIAL_NUMBER","ESN")
      df.join(ref_first_connection,df("ENGINE_DETAILS_ESN")===ref_first_connection("ESN"),"left")
        .drop("ESN")
    }

    Util.saveData(Util.formatCapitalizeNames(getResult(engineFeaturesMasterDF)), cfg(Constants.FET_DB), cfg(Constants.REL_ENG_FET_TBL), cfg(Constants.HDFS_PATH))


  }


}
