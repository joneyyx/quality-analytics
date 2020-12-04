package mck.qb.columbia.primary.telematics

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.Util.formatCapitalizeNames
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


object TelFaultCodePrimaryJob extends Job {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.PRI_TEL_FC_DTL)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  /**
    * Intended to be implemented for every job created.
    */
  override def run(): Unit = {
    val telFaultCodeDF: DataFrame = getTelFaultCodeDF()
      .where(year(current_date()) >= year($"Occurrence_Date_Time") and year($"Occurrence_Date_Time") >= year(current_date())-2  )
      .withColumnRenamed("Engine_Total_Hours_of_Operation","WORK_HOUR")

    val esn_master = spark.table(s"${cfg(Constants.PRI_TEL_DB)}.${cfg(Constants.ESN_MST_DTL_TBL)}")
      .select("ESN","ENGINE_NAME","partition")
      .withColumnRenamed("ESN","Engine_Serial_Number")
      .coalesce(1)

    val result = telFaultCodeDF.join(esn_master, Seq("Engine_Serial_Number"), "left")
//      .withColumn("Occurrence_Date_Time",Util.datetimeUTC2CST($"Occurrence_Date_Time"))
      .withColumn("report_date",date_trunc("yyyymmdd",Util.datetimeUTC2CST($"Occurrence_Date_Time")))
//      .withColumn("Sent_Date_Time",Util.datetimeUTC2CST($"Sent_Date_Time"))
//      .withColumn("Response_Trigger_Date_Time",Util.datetimeUTC2CST($"Response_Trigger_Date_Time"))
//      .withColumn("Processing_End_Date_Time",Util.datetimeUTC2CST($"Processing_End_Date_Time"))
//      .withColumn("Primary_Occurrence_Date_Time",Util.datetimeUTC2CST($"Primary_Occurrence_Date_Time"))
//    if (cfg(Constants.DATA_SAMPLE).equalsIgnoreCase("true")) {
//      logger.warn(s"INFO :: Saving sample data(0.0001) >>>>>>>> %s.%s".format(cfg(Constants.PRI_DB), cfg(Constants.TEL_FC_DTL_TBL)))
//      Util.saveData(telFaultCodeDF.sample(0.001), cfg(Constants.PRI_DB), cfg(Constants.TEL_FC_DTL_TBL), cfg(Constants.HDFS_PATH))
//    } else {

//    Util.saveDataWithPartition(
//      result,
//      cfg(Constants.PRI_DB),
//      cfg(Constants.TEL_FC_DTL_TBL),
//      cfg(Constants.HDFS_PATH)
//    )("OCCURRENCE_DATE_TIME")


    Util.saveData(result,cfg(Constants.PRI_DB), cfg(Constants.TEL_FC_DTL_TBL), cfg(Constants.HDFS_PATH))


//    }
  }
//  def saveData(df: DataFrame, db: String, tbl: String, path: String, mode: String = "overwrite", format: String = "parquet"): Unit = {
//    formatCapitalizeNames(df).write.
//      options(Map(Constants.PATH -> Util.getWarehousePath(path, db, tbl))).
//      mode(mode).format(format).partitionBy("partition_name","Occurrence_Date_Time").
//      saveAsTable(s"$db.$tbl")
//  }

  private def getTelFaultCodeDF(): DataFrame = {
    val telFaultCodeSchema: Map[String, DataType] = Map(
      "Schema_Version" -> StringType,
      "Notification_Version" -> StringType,
      "Message_Type" -> StringType,
      "Telematics_Partner_Message_ID" -> StringType,
      "Telematics_Partner_Name" -> StringType,
      "Occurrence_Date_Time" -> StringType,
      "Sent_Date_Time" -> StringType,
      "Response_Trigger_Date_Time" -> StringType,
      "Processing_End_Date_Time" -> StringType,
      "Sent_To_Occurrence_Duration" -> StringType,
      "Received_To_Sent_Duration" -> StringType,
      "Processing_Start_To_Received_Duration" -> StringType,
      "Response_Trigger_To_Processing_Start_Duration" -> StringType,
      "Processing_End_To_Response_Trigger_Duration" -> StringType,
      "Processing_End_To_Processing_Start_Duration" -> StringType,
      "Error_Code" -> StringType,
      "Error_Information" -> StringType,
      "Response_Version" -> StringType,
      "Notification_ID" -> StringType,
      "Customer_Reference" -> StringType,
      "Customer_Equipment_Group" -> StringType,
      "CUST_ID" -> StringType,
      "CUST_REF_NAME" -> StringType,
      "CUST_ACCT_NAME" -> StringType,
      "CUST_NOTIF_EMAIL_NAME" -> StringType,
      "CUST_ADDRESS_LINE1_NAME" -> StringType,
      "CUST_ADDRESS_LINE2_NAME" -> StringType,
      "CUST_ADDRESS_LINE3_NAME" -> StringType,
      "CUST_CITY_NAME" -> StringType,
      "CUST_STATE_NAME" -> StringType,
      "CUST_ZIP_CD" -> StringType,
      "CUST_COUNTRY_NAME" -> StringType,
      "CUST_EQUIP_GROUP_NAME" -> StringType,
      "COUNTRY_CD" -> StringType,
      "TSP_NAME" -> StringType,
      "FIRST_NAME" -> StringType,
      "LAST_NAME" -> StringType,
      "CUST_CONTACT_EMAIL_NAME" -> StringType,
      "BCC_NOTIF_EMAIL_NAME" -> StringType,
      "REGION_NAME" -> StringType,
      "LANG_NAME" -> StringType,
      "CUST_CONTACT_PHN_NUM" -> StringType,
      "CARE_SERVICE_LEVEL_DESC" -> StringType,
      "BUSINESS_ENTITY_NAME" -> StringType,
      "SERVICE_LOC_BUSINESS_ENTITY_NAME" -> StringType,
      "SERVICE_LOC_LANG_NAME" -> StringType,
      "SERVICE_LOC_REGION_NAME" -> StringType,
      "Equipment_ID" -> StringType,
      "VIN" -> StringType,
      "Vehicle_Model" -> StringType,
      "Gross_Vehicle_Weight" -> StringType,
      "Driven_Equipment" -> StringType,
      "OEM" -> StringType,
      "Horsepower" -> StringType,
      "Chassis" -> StringType,
      "Drive" -> StringType,
      "Equipment_SubApplication" -> StringType,
      "Telematics_Box_ID" -> StringType,
      "Telematics_Box_Hardware_Variant" -> StringType,
      "Telematics_Box_Software_Version" -> StringType,
      "Latitude" -> DoubleType,
      "Longitude" -> DoubleType,
      "Altitude" -> DoubleType,
      "Direction_Heading" -> StringType,
      "Direction_Heading_Orientation" -> StringType,
      "Location_Text_Description" -> StringType,
      "Direction_Heading_Degree" -> StringType,
      "GPS_Vehicle_Speed" -> StringType,
      "Engine_Serial_Number" -> IntegerType,
      "Datalink_Bus" -> StringType,
      "Vehicle_Distance" -> StringType,
      "Service_Model_Name" -> StringType,
      "Service_Locator_Market_Application" -> StringType,
      "Source_Address" -> StringType,
      "Service_Locator_Link" -> StringType,
      "General_Assistance_PhoneNumber" -> StringType,
      "URL_Cummins_General" -> StringType,
      "URL_2" -> StringType,
      "URL_3" -> StringType,
      "URL_4" -> StringType,
      "Cummins_Name" -> StringType,
      "Active" -> StringType,
      "SPN" -> StringType,
      "FMI" -> StringType,
      "Occurrence_Count" -> StringType,
      "Fault_Code" -> StringType,
      "Fault_Code_Description" -> StringType,
      "Instruction_To_Fleet_Mgr" -> StringType,
      "Instruction_To_Operator" -> StringType,
      "Additional_Info_To_Operator" -> StringType,
      "Derate_Flag" -> StringType,
      "Derate_Value1" -> StringType,
      "Derate_Value2" -> StringType,
      "Derate_Value3" -> StringType,
      "Derate_Group1" -> StringType,
      "Derate_Severity1" -> StringType,
      "Derate_Group2" -> StringType,
      "Derate_Severity2" -> StringType,
      "Derate_Group3" -> StringType,
      "Derate_Severity3" -> StringType,
      "Lamp_Device" -> StringType,
      "Lamp_Color" -> StringType,
      "Report_Type" -> StringType,
      "Fault_Root_Cause1" -> StringType,
      "Fault_Likelihood1" -> StringType,
      "Fault_Root_Cause2" -> StringType,
      "Fault_Likelihood2" -> StringType,
      "Fault_Root_Cause3" -> StringType,
      "Fault_Likelihood3" -> StringType,
      "Fault_Root_Cause4" -> StringType,
      "Fault_Likelihood4" -> StringType,
      "Shutdown_Flag" -> StringType,
      "Shutdown_Description" -> StringType,
      "Priority" -> StringType,
      "Fault_Code_Category" -> StringType,
      "Additional_Diagnostic_Info" -> StringType,
      "Primary_Fault_Code" -> StringType,
      "Primary_SPN" -> StringType,
      "Primary_FMI" -> StringType,
      "Primary_Occurrence_Date_Time" -> StringType,
      "Primary_Fault_Code_Description" -> StringType,
      "Related" -> StringType,
      "Snapshot_DateTimestamp" -> StringType,
      "A_C_High_Pressure_Fan_Switch" -> StringType,
      "Brake_Switch" -> StringType,
      "Clutch_Switch" -> StringType,
      "Cruise_Control_Enable_Switch" -> StringType,
      "Parking_Brake_Switch" -> StringType,
      "PTO_Governor_State" -> StringType,
      "Water_In_Fuel_Indicator_1" -> StringType,
      "Calibration_Identification" -> StringType,
      "Calibration_Verification_Number" -> StringType,
      "Number_of_Software_Identification_Fields" -> StringType,
      "Software_Identification" -> StringType,
      "Make" -> StringType,
      "Model" -> StringType,
      "Unit_Number_Power_Unit" -> StringType,
      "Engine_Operating_State" -> StringType,
      "Engine_Torque_Mode" -> StringType,
      "Engine_Amber_Warning_Lamp_Command" -> StringType,
      "Engine_Red_Stop_Lamp_Command" -> StringType,
      "OBD_Malfunction_Indicator_Lamp_Command" -> StringType,
      "Aftertreatment_1_Intake_Dew_Point" -> StringType,
      "Aftertreatment_1_Exhaust_Dew_Point" -> StringType,
      "Accelerator_Interlock_Switch" -> StringType,
      "DPF_Thermal_Management_Active" -> StringType,
      "Cruise_Control_Active" -> StringType,
      "Fan_Drive_State" -> StringType,
      "Diesel_Particulate_Filter_Status" -> StringType,
      "SCR_Thermal_Management_Active" -> StringType,
      "Aftertreatment_1_SCR_System_State" -> StringType,
      "Aftertreatment_SCR_Operator_Inducement_Severity" -> StringType,
      "Diesel_Particulate_Filter_Regeneration_Inhibit_Switch" -> StringType,
      "Diesel_Particulate_Filter_Regeneration_Force_Switch" -> StringType,
      "Accelerator_Pedal_Position_1" -> StringType,
      "Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Level" -> StringType,
      "Aftertreatment_1_Outlet_NOx_1" -> StringType,
      "Aftertreatment_1_SCR_Intake_Temperature" -> StringType,
      "Aftertreatment_1_SCR_Outlet_Temperature" -> StringType,
      "Ambient_Air_Temperature" -> StringType,
      "Barometric_Pressure" -> StringType,
      "Battery_Potential_Power_Input_1" -> StringType,
      "Commanded_Engine_Fuel_Rail_Pressure" -> StringType,
      "Engine_Coolant_Level_1" -> StringType,
      "Engine_Fuel_Rate" -> StringType,
      "Engine_Oil_Temperature_1" -> StringType,
      "Engine_Speed" -> StringType,
      "Engine_Total_Fuel_Used" -> StringType,
      "Engine_Total_Hours_of_Operation" -> StringType,
      "Total_ECU_Run_Time" -> StringType,
      "Wheel_Based_Vehicle_Speed" -> StringType,
      "Actual_Engine_Percent_Torque_Fractional" -> StringType,
      "Actual_Maximum_Available_Engine_Percent_Torque" -> StringType,
      "Engine_Derate_Request" -> StringType,
      "Engine_Fan_1_Requested_Percent_Speed" -> StringType,
      "Engine_Total_Idle_Fuel_Used" -> StringType,
      "Engine_Total_Idle_Hours" -> StringType,
      "Engine_Trip_Fuel" -> StringType,
      "Fan_Speed" -> StringType,
      "Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Heater" -> StringType,
      "Engine_Exhaust_Gas_Recirculation_1_Mass_Flow_Rate" -> StringType,
      "Engine_Intake_Air_Mass_Flow_Rate" -> StringType,
      "Transmission_Actual_Gear_Ratio" -> StringType,
      "Engine_Throttle_Valve_1_Position_1" -> StringType,
      "Aftertreatment_1_Diesel_Oxidation_Catalyst_Intake_Gas_Temperature" -> StringType,
      "Aftertreatment_1_SCR_Conversion_Efficiency" -> StringType,
      "Diesel_Particulate_Filter_1_Ash_Load_Percent" -> StringType,
      "Aftertreatment_1_Diesel_Particulate_Filter_Intake_Gas_Temperature" -> StringType,
      "Aftertreatment_1_Diesel_Particulate_Filter_Outlet_Gas_Temperature" -> StringType,
      "Diesel_Particulate_Filter_Outlet_Pressure_1" -> StringType,
      "Aftertreatment_1_SCR_Intake_Nox_1" -> StringType,
      "Aftertreatment_1_Diesel_Particulate_Filter_Differential_Pressure" -> StringType,
      "Aftertreatment_1_Diesel_Exhaust_Fluid_Actual_Quantity_of_Integrator" -> StringType,
      "Engine_Exhaust_Bank_1_Pressure_Regulator_Position" -> StringType,
      "Engine_Exhaust_Manifold_Bank_1_Flow_Balance_Valve_Actuator_Control" -> StringType,
      "Diesel_Particulate_Filter_1_Soot_Density" -> StringType,
      "Aftertreatment_1_Total_Fuel_Used" -> StringType,
      "Aftertreatment_1_Diesel_Exhaust_Fluid_Concentration" -> StringType,
      "Actual_Engine_Percent_Torque" -> StringType,
      "Aftertreatment_1_Diesel_Exhaust_Fluid_Tank_Temperature" -> StringType,
      "Cruise_Control_Set_Speed" -> StringType,
      "Drivers_Demand_Engine_Percent_Torque" -> StringType,
      "Engine_Coolant_Temperature" -> StringType,
      "Engine_Intake_Manifold_1_Pressure" -> StringType,
      "Engine_Intake_Manifold_1_Temperature" -> StringType,
      "Engine_Oil_Pressure" -> StringType,
      "Engine_Percent_Load_At_Current_Speed" -> StringType,
      "Engine_Reference_Torque" -> StringType,
      "Nominal_Friction_Percent_Torque" -> StringType,
      "Time_Since_Engine_Start" -> StringType,
      "Engine_Demand_Percent_Torque" -> StringType,
      "Engine_Total_Revolutions" -> StringType,
      "Mileage" -> IntegerType,
      "Gross_Combination_Weight" -> StringType,
      "Unfiltered_Raw_Vehicle_Weight" -> StringType,
      "Diesel_Particulate_Filter_1_Time_Since_Last_Active_Regeneration" -> StringType,
      "Diesel_Exhaust_Fluid_Quality_Malfunction_Time" -> StringType,
      "Diesel_Exhaust_Fluid_Tank_1_Empty_Time" -> StringType,
      "Engine_Exhaust_Pressure_1" -> StringType,
      "SCR_Operator_Inducement_Total_Time" -> StringType,
      "Aftertreatment_1_Total_Regeneration_Time" -> StringType,
      "Aftertreatment_1_Total_Disabled_Time" -> StringType,
      "Aftertreatment_1_Total_Number_of_Active_Regenerations" -> StringType,
      "Aftertreatment_1_Diesel_Particulate_Filter_Total_Number_of_Active_Regeneration_Inhibit_Requests" -> StringType,
      "Aftertreatment_1_Diesel_Particulate_Filter_Total_Number_of_Active_Regeneration_Manual_Requests" -> StringType,
      "Aftertreatment_1_Average_Time_Between_Active_Regenerations" -> StringType,
      "Aftertreatment_1_Diesel_Exhaust_Fluid_Doser_Absolute_Pressure" -> StringType,
      "Displacement" -> StringType,
      "Engine_Model" -> StringType,
      "Gradient" -> StringType,
      "Accel_Rate" -> StringType,
      "Fuel_TANK_Level" -> StringType,
      "Rear_Axle_Ratio" -> StringType,
      "Transmission_Model" -> StringType,
      "Tire_Model" -> StringType
    )


    val fc: DataFrame = spark.table(s"${cfg(Constants.RAW_DB)}.${cfg(Constants.RAW_TEL_TBL)}")
      .withColumnRenamed("High_Resolution_Total_Vehicle_Distance","Mileage")
    val fc_err: DataFrame = spark.table(s"${cfg(Constants.RAW_DB)}.${cfg(Constants.RAW_TEL_ERR_TBL)}")
      .withColumnRenamed("High_Resolution_Total_Vehicle_Distance","Mileage")


    Util.applySchema(fc.union(fc_err), telFaultCodeSchema)
  }
}
