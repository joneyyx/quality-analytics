package mck.qb.columbia.primary.reliability

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
  * xxx
  * creat by newforesee 2019-03-29
  */
object RelEngineDetailsPrimaryJob extends Job {

  val myConf: Config = AppConfig.getMyConfig(Constants.PRI_REL_ENG_DTL)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {
    val relEngineDetailDF: DataFrame = getRelEngineDetailDF
//    if (cfg(Constants.DATA_SAMPLE).equalsIgnoreCase("true")) {
//      logger.warn(s"INFO :: Saving sample data(0.0001) >>>>>>>> %s.%s".format(cfg(Constants.PRI_DB), cfg(Constants.REL_ENG_DTL_TBL)))
//
//      Util.saveData(relEngineDetailDF.sample(0.001), cfg(Constants.PRI_DB), cfg(Constants.REL_ENG_DTL_TBL), cfg(Constants.HDFS_PATH))
//    }else{
      Util.saveData(relEngineDetailDF, cfg(Constants.PRI_DB), cfg(Constants.REL_ENG_DTL_TBL), cfg(Constants.HDFS_PATH))
//    }
  }

  private def getRelEngineDetailDF(): DataFrame = {

    val relEngineDetailSchema = Map(
      "TERRITORY_CODE" -> StringType,
      "E_W_Zone" -> StringType,
      "ESN" -> IntegerType,
      "ESN_NEW" -> IntegerType,
      "Engine_Model" -> StringType,
      "SO_NUM" -> StringType,
      "Build_Date" -> TimestampType,
      "Engine_Family" -> StringType,
      "Displacement" -> FloatType,
      "Emission_Level" -> StringType,
      "ATS_Type" -> StringType,
      "Horsepower" -> StringType,
      "USERAPP" -> StringType,
      "Vehicle_Type" -> StringType,
      "OEM_Code" -> StringType,
      "OEM_Name" -> StringType,
      "ESN_IN_SERVICE_DATE" -> StringType,
      "ENTITY_CODE" -> StringType,
      "In_Service_Date" -> StringType,
      "Engine_Ship_Date" -> StringType,
      "Engine_Plant" -> StringType,
      "Engine_Retirement_Date" -> StringType,
      "Time_Lag_From_Build_To_In_Service" -> IntegerType,
      "In_Service_Date_Ave_Time_Lag" -> TimestampType,
      "Dom_Int" -> StringType,
      "Engine_Type_Desc" -> StringType,
      "Warehouse" -> StringType,
      "Warehouse_in_Type" -> StringType,
      "Warehouse_in_Owner" -> StringType,
      "Warehouse_out_Type" -> StringType,
      "Warehouse_out_Time" -> StringType,
      "Warehouse_out_Owner" -> StringType,
      "Code" -> StringType,
      "OEM_English_Name" -> StringType,
      "New_Department" -> StringType,
      "Region_Category" -> StringType,
      "Large_Region" -> StringType,
      "Marketing_Category" -> StringType,
      "Distributor" -> StringType,
      "Vehicle_Model" -> StringType,
      "VIN" -> StringType,
      "Engine_Model_ESN" -> StringType,
      "Vehicle" -> StringType,
      "Body_Status" -> StringType,
      "Vehicle_Series1" -> StringType,
      "Series" -> StringType,
      "Drive" -> StringType,
      "BridgE_spEEd_ratio" -> StringType,
      "Engine_Type" -> StringType,
      "HP_period1" -> StringType,
      "HP_period2" -> StringType,
      "Wheelbase" -> StringType,
      "Vehicle_length" -> StringType,
      "Vehicle_Length_Period" -> StringType,
      "Vehicle_Category" -> StringType,
      "In_Service_Month" -> StringType,
      "Count" -> StringType,
      "Region" -> StringType,
      "Actual_In_Service_Date" -> TimestampType,
      "Ship_Channel" -> StringType,
      "Calibration_Version" -> StringType,
      "GAS_Category" -> StringType,
      "Base_Engine_Arrive_Date" -> TimestampType,
      "Plant_ID_Code_" -> StringType,
      "FAMBV" -> StringType,
      "Design_Phase_Code" -> StringType,
      "VARBV" -> StringType,
      "MKTG_RPM" -> StringType,
      "DESIGN_APPLICATION_CODE" -> StringType,
      "Global_Region" -> StringType,
      "Territory" -> StringType,
      "CPL" -> StringType,
      "MKTGCONFIG" -> StringType,
      "MKTG_CONFIG_NAME" -> StringType,
      "COVCODE" -> StringType,
      "DESIGNCONFIG" -> StringType,
      "INVOICE_AMOUNT" -> StringType,
      "Engine_Platform" -> StringType,
      "Engine_Group_Desc" -> StringType,
      "ABO_Quality_Performance_Group" -> StringType,
      "Engine_Usage" -> StringType,
      "CCI_Channel_Sales_Y_N" -> StringType,
      "Year" -> StringType
    )


    val strSQLBuild: String =
      s"""SELECT
         | `TERRITORY_CODE` as TERRITORY_CODE,
         | `东区_西区_E_W_Zone` as E_W_Zone,
         | `发动机号_ESN` as ESN,
         | `发动机号_ESN_NEW` as ESN_NEW,
         | `机型_Engine_Model` as Engine_Model,
         | `订单号_SO` as SO_NUM,
         | case when length(`生产日期_Build_Date`)!=0 then `生产日期_Build_Date` else '1900-01-01' end as Build_Date,
         | `发动机系列_Engine_Family` as Engine_Family,
         | `排量_Displacement` as Displacement,
         | `排放_Emission_Level` as Emission_Level,
         | `后处理类型_ATS_Type` as ATS_Type,
         | `马力_Horsepower` as Horsepower,
         | `发动机应用领域_USERAPP` as USERAPP,
         | `细分市场_Vehicle_Type` as Vehicle_Type,
         | `OEM代码_OEM_Code` as OEM_Code,
         | `OEM名称_OEM_Name` as OEM_Name,
         | case when length(`ESN_IN_SERVICE_DATE`)=0 then '1900-01-01' else `ESN_IN_SERVICE_DATE` end as ESN_IN_SERVICE_DATE,
         | `ENTITY_CODE` as ENTITY_CODE,
         | case when length(`保修起始日_In_Service_Date`)=0 then '1900-01-01' else `保修起始日_In_Service_Date` end as In_Service_Date,
         | case when length(`发动机发运日期_Engine_Ship_Date`)!=0 then `发动机发运日期_Engine_Ship_Date` else '1900/01/01' end as Engine_Ship_Date,
         | `发动机厂_Engine_Plant` as Engine_Plant,
         | case when length(`发动机报废日期_Engine_Retirement_Date`)=0 then '1900-01-01' else `发动机报废日期_Engine_Retirement_Date` end as Engine_Retirement_Date,
         | `时间间隔_建造到开始服务_Time_Lag_From_Build_To_In_Service` as Time_Lag_From_Build_To_In_Service,
         | `开始服务时间_In_Service_Date_基于平均时间间隔_Ave_Time_Lag` as In_Service_Date_Ave_Time_Lag,
         | `国内国外_Dom_Int` as Dom_Int,
         | `型号描述_Engine_Type` as Engine_Type_Desc,
         | `仓库_Warehouse` as Warehouse,
         | `入库类型_Warehouse_in_Type` as Warehouse_in_Type,
         | `入库操作人_Warehouse_in_Owner` as Warehouse_in_Owner,
         | `出库类型_Warehouse_out_Type` as Warehouse_out_Type,
         | case when length(`出库时间_Warehouse_out_Time`)=0 then '1900-01-01 00:00:00' else `出库时间_Warehouse_out_Time` end as Warehouse_out_Time,
         | `出库操作人_Warehouse_out_Owner` as Warehouse_out_Owner,
         | `条码_Code` as Code,
         | `OEM名称_英_OEM_Name` as OEM_English_Name,
         | `新部门_New_Department` as New_Department,
         | `区域分类_Region_Category` as Region_Category,
         | `大区_Region` as Large_Region,
         | `市场部_Marketing_Category` as Marketing_Category,
         | `经销商_Distributor` as Distributor,
         | `车型_Vehicle_Model` as Vehicle_Model,
         | `出厂编号_VIN` as VIN,
         | `Engine_Model_ESN` as Engine_Model_ESN,
         | `车身_Vehicle` as Vehicle,
         | `车身状态_Body_Status` as Body_Status,
         | `版次1_Vehicle_Series1` as Vehicle_Series1,
         | `系列_Series` as Series,
         | `驱动_Drive` as Drive,
         | `桥_速比_BridgE_spEEd_ratio` as BridgE_spEEd_ratio,
         | `发动机_Engine_Type` as Engine_Type,
         | `重点动力_HP_period1` as HP_period1,
         | `马力段_HP_period2` as HP_period2,
         | `轴距_Wheelbase` as Wheelbase,
         | `厢长_Vehicle_length` as Vehicle_length,
         | `厢长段_Vehicle_Length_Period` as Vehicle_Length_Period,
         | `类别_Vehicle_Category` as Vehicle_Category,
         | `In_Service_Month` as In_Service_Month,
         | `Count` as Count,
         | `区域_Region` as Region,
         | case when length(`实际保修起始日_Actual_In_Service_Date`)=0 then '1900-01-01' else `实际保修起始日_Actual_In_Service_Date` end as Actual_In_Service_Date,
         | `发运渠道_Ship_Channel` as Ship_Channel,
         | `标定版本号_Calibration_Version` as Calibration_Version,
         | `气源类型_GAS_Category`  as GAS_Category,
         | case when length(`基础机到货日期_Base_Engine_Arrive_Date`)=0 then '1900-01-01' else `基础机到货日期_Base_Engine_Arrive_Date` end as Base_Engine_Arrive_Date,
         | `工厂代码_Plant_ID_Code` as Plant_ID_Code_,
         | `FAMBV` as FAMBV,
         | `设计阶段代码_Design_Phase_Code` as Design_Phase_Code,
         | `VARBV` as VARBV,
         | `转速_MKTG_RPM` as MKTG_RPM,
         | `DESIGN_APPLICATION_CODE` as DESIGN_APPLICATION_CODE,
         | `全球区域_Global_Region` as Global_Region,
         | `发运国家与地区_Territory` as Territory,
         | `CPL` as CPL,
         | `MKTGCONFIG` as MKTGCONFIG,
         | `MKTG_CONFIG_NAME` as MKTG_CONFIG_NAME,
         | `COVCODE` as COVCODE,
         | `DESIGNCONFIG` as DESIGNCONFIG,
         | `INVOICE_AMOUNT` as INVOICE_AMOUNT,
         | `发动机平台_Engine_Platform` as Engine_Platform,
         | `发动机类别_Engine_Group_Desc` as Engine_Group_Desc,
         | `ABO质量绩效分组_ABO_Quality_Performance_Group` as ABO_Quality_Performance_Group,
         | `发动机用途_Engine_Usage` as Engine_Usage,
         | `是否CCI渠道销售_CCI_Channel_Sales_Y_N` as CCI_Channel_Sales_Y_N,
         | Year Year
         |  FROM ${cfg(Constants.RAW_DB)}.${cfg(Constants.RAW_REL_BLD_TBL)}
         |  """.stripMargin

    //val relEngineDetailDF: DataFrame = Util.applySchema(spark.sql(strSQLBuild),relEngineDetailSchema)
    Util.applySchema(spark.sql(strSQLBuild),relEngineDetailSchema)
  }
}
