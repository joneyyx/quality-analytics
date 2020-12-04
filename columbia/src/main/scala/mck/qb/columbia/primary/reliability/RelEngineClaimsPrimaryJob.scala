package mck.qb.columbia.primary.reliability

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object RelEngineClaimsPrimaryJob extends Job{

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.PRI_REL_ENG_CLM)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  override def run(): Unit = {


    // Format & Rename cols
    val relEngineClaimDF: Dataset[Row] = getRelEngineClaimDetailDF()
      .where("to_date(failure_date)<CURRENT_DATE()")

    val relEngineClaimFormatedDF = relEngineClaimDF.
      withColumn("Pay_Date", regexp_replace($"Pay_Date", "/", "-")).
      withColumn("CLAIM_SUBMIT_DATE", regexp_replace($"CLAIM_SUBMIT_DATE", "/", "-")).
      withColumn("Failure_Part",when($"Failure_Part".isNotNull || $"Failure_Part"=!="",$"Failure_Part").otherwise(lit("Unknow"))).
      withColumn("ABO_Failure_Part",when($"ABO_Failure_Part".isNotNull || $"ABO_Failure_Part"=!="",$"ABO_Failure_Part").otherwise(lit("Unknow")))

    //CLAIM_NUMBER去重，如果pay_date有值且不等于1900/01/01 or 1900-01-01，保留最新的pay_date行； 如果pay_date无值，随机保留一行；
    relEngineClaimFormatedDF.
      withColumn("TMP_PAY_DATE", to_date(unix_timestamp(col("PAY_DATE"), "yyyy-mm-dd").cast("timestamp"))).
      createOrReplaceTempView("relEngineClaimFormatedDF")
    val strSql: String =
      """
        |SELECT * FROM
        |(
        |SELECT *,
        |ROW_NUMBER() OVER(PARTITION BY CLAIM_NUMBER ORDER BY TMP_PAY_DATE DESC) RANK
        |FROM relEngineClaimFormatedDF
        |)
        |WHERE RANK=1
        |
      """.stripMargin
    val relEngineClaimFormatedDFclean = spark.sql(strSql).
      drop("TMP_PAY_DATE").
      drop("RANK")
//    if (cfg(Constants.DATA_SAMPLE).equalsIgnoreCase("true")) {
//      logger.warn(s"INFO :: Saving sample data(0.0001) >>>>>>>> %s.%s".format(cfg(Constants.PRI_DB), cfg(Constants.REL_ENG_CLM_DTL_TBL)))
//      Util.saveData(relEngineClaimFormatedDFclean.sample(0.001), cfg(Constants.PRI_DB), cfg(Constants.REL_ENG_CLM_DTL_TBL), cfg(Constants.HDFS_PATH))
//    }else{
      Util.saveData(relEngineClaimFormatedDFclean, cfg(Constants.PRI_DB), cfg(Constants.REL_ENG_CLM_DTL_TBL), cfg(Constants.HDFS_PATH))
//    }

  }

  /**
    * Certain schema and define it,then read Hive table in data frame
    * while rename and applying the schema on top of it.
    * @return
    */
  private def getRelEngineClaimDetailDF(): DataFrame = {

    val RelEngineClaimDetailSchema = Map(
      "Claim_Number" -> StringType,
      "Coverage_Entity" -> StringType,
      "Coverage_Program" -> StringType,
      "Whole_Engine_Claim_or_not" -> StringType,
      "Claim_submit_date" -> StringType,
      "Change_part_flag" -> StringType,
      "Third_Party_Claim_ID" -> StringType,
      "Assessment_Progress" -> StringType,
      "Dealer_Name" -> StringType,
      "Dealer_Code" -> StringType,
      "ESN" -> IntegerType,
      "ESN_NEW" -> IntegerType,
      "SO" -> StringType,
      "Engine_Power" -> StringType,
      "Engine_Type" -> StringType,
      "Build_Date" -> StringType,
      "USERAPP" -> StringType,
      "Mileage_or_Hour" -> StringType,
      "OEM_Code" -> StringType,
      "OEM_Name" -> StringType,
      "Vehicle_Model" -> StringType,
      "Chassis_Number" -> StringType,
      "Vehicle_Build_Date" -> TimestampType,
      "Vehicle_Purchase_Date" -> TimestampType,
      "Failure_Date" -> TimestampType,
      "Fail_Code" -> StringType,
      "Fail_Code_Description" -> StringType,
      "ComplaIntegerType" -> StringType,
      "Failure_Cause" -> StringType,
      "Correction" -> StringType,
      "Accountable_Dept" -> StringType,
      "Failure_Part_Number" -> StringType,
      "Failure_Part" -> StringType,
      "Failure_Part_Supplier_Code" -> StringType,
      "Failure_Part_Supplier" -> StringType,
      "Total_Emergent_Cost" -> StringType,
      "Emergent_Material_Cost" -> StringType,
      "Emergent_Material_Cost_Audit" -> StringType,
      "Total_Labor_Cost" -> StringType,
      "Total_Labor_Cost_Audit" -> StringType,
      "Labor_Hour" -> DoubleType,
      "Total_Submitted_Material_Cost" -> StringType,
      "Total_Audit_Material_Cost" -> StringType,
      "Total__Submitted_Warranty_Cost" -> StringType,
      "Total_Audit_Warranty_Cost" -> StringType,
      "Audit_Status" -> StringType,
      "Pay_Date" -> StringType,
      "Submit_to_Finance_Date" -> StringType,
      "Total_Cost" -> DoubleType,
      "Claim_Channel" -> StringType,
      "Pay_Month" -> StringType,
      "Pay_Year" -> StringType,
      "Audit_Emergent_Total_Cost" -> StringType,
      "Region" -> StringType,
      "Province" -> StringType,
      "Batch_Number" -> StringType,
      "Total_Submitted_Labor_Cost" -> StringType,
      "Total_Audit_Labor_Cost" -> StringType,
      "Material_Cost_Without_Management_Fee" -> StringType,
      "Audit_Type" -> StringType,
      "Aftertreament_Purchasing_Type" -> StringType,
      "ENTITY_CODE" -> StringType,
      "Base_or_ATS" -> StringType,
      "No" -> StringType,
      "Displacement" -> StringType,
      "Emission_Level" -> StringType,
      "ATS_Type" -> StringType,
      "Dongfeng_None_Dongfeng" -> StringType,
      "UA2_Blockage" -> StringType,
      "FC2772" -> StringType,
      "Returned_Part_Analysis" -> StringType,
      "MILEAGE_DISTRIBUTION_KEY" -> StringType,
      "HOUR_DISTRIBUTION_KEY" -> StringType,
      "BIS_AIS" -> StringType,
      "attribute" -> StringType,
      "In_Service_Date" -> TimestampType,
      "Vehicle_Type" -> StringType,
      "device_model" -> StringType,
      "Mileage" -> IntegerType,
      "Final_travel_amount_total" -> StringType,
      "Final_other_amount_total" -> StringType,
      "Total_deduction" -> StringType,
      "Reason_for_deduction" -> StringType,
      "Declaration_note" -> StringType,
      "Initial_review_notes" -> StringType,
      "Final_review_note" -> StringType,
      "Rebate_note" -> StringType,
      "Number_of_submissions" -> StringType,
      "Part_status" -> StringType,
      "SO_status" -> StringType,
      "Old_flow_number" -> StringType,
      "New_flow_number" -> StringType,
      "Invoice_Total_AMT" -> StringType,
      "OEM_Parts_NO" -> StringType,
      "CES_Parts_NO" -> StringType,
      "Customer" -> StringType,
      "Claim_Reasonable_or_Not" -> StringType,
      "Claim_Abnormal_Comments" -> StringType,
      "Apeal_or_Not" -> StringType,
      "Appeal_Reason" -> StringType,
      "Appeal_Result" -> StringType,
      "Appeal_Success_AMT_Without_Tax" -> StringType,
      "Prime_Failure_Mode" -> StringType,
      "NO_CES_Responsibiliy" -> StringType,
      "Payment_Difference" -> StringType,
      "Policy_in_BFDA" -> StringType,
      "Key_OEM" -> StringType,
      "IPTV3" -> StringType,
      "Parts_Production_Date" -> StringType,
      "Part_SN" -> StringType,
      "Failure_Quantity" -> StringType,
      "Description_level" -> StringType,
      "MIB_Day" -> StringType,
      "MIB_Month" -> StringType,
      "Total_AMT" -> StringType,
      "Appeal_Success_AMT_With_Tax" -> StringType,
      "PR_No" -> StringType,
      "Appeal_No" -> StringType,
      "Image" -> StringType,
      "Warranty_Mon" -> StringType,
      "User_opinion" -> StringType,
      "username" -> StringType,
      "User_phone" -> StringType,
      "User_units" -> StringType,
      "New_part_has_been_running_hours" -> StringType,
      "Mileage_of_new_parts" -> StringType,
      "New_part_warranty_start_date" -> StringType,
      "Repair_location" -> StringType,
      "Number_of_repairs" -> StringType,
      "level_of_damage" -> StringType,
      "First_failure_hour" -> StringType,
      "First_fault_mileage" -> StringType,
      "Mobile_phone" -> StringType,
      "Audit_note" -> StringType,
      "Device_name" -> StringType,
      "Settlement_batch" -> StringType,
      "Failure_Part_English" -> StringType,
      "Work_Hour" -> IntegerType,
      "Job_file_number" -> StringType,
      "Engineer_review_notes" -> StringType,
      "Payment_date" -> StringType,
      "service_personnel" -> StringType,
      "Service_start_time" -> StringType,
      "Service_end_time" -> StringType,
      "service_line" -> StringType,
      "Amount_to_be_transferred" -> StringType,
      "CAC_initial_review_return_number" -> StringType,
      "CAC_Engineer_Reviewer" -> StringType,
      "CAC_Engineer_Review_Time" -> StringType,
      "CAC_Engineer_Retirement_Notes" -> StringType,
      "CAC_engineer_refunds" -> StringType,
      "CAC_engineer_return_time" -> StringType,
      "Initial_review_travel_expenses" -> StringType,
      "Initial_review_part_fee" -> StringType,
      "Initial_review_other_fees" -> StringType,
      "First_instance" -> StringType,
      "First_trial_pass_time" -> StringType,
      "Initial_review_time" -> StringType,
      "Reverse_management_fee" -> StringType,
      "Inverted_review_comments" -> StringType,
      "Inverted_state" -> StringType,
      "Old_parts_warranty_start_date" -> StringType,
      "Old_parts_mileage" -> StringType,
      "Old_parts_have_been_running_hours" -> StringType,
      "Entering_travel_expenses" -> StringType,
      "Entering_working_hours" -> StringType,
      "Enter_other_fees" -> StringType,
      "Final_approval_time" -> StringType,
      "Final_review_reimbursement_note" -> StringType,
      "Final_review_order_number" -> StringType,
      "Final_review_time" -> StringType,
      "Data_Month" -> StringType,
      "Dom_or_IntegerType" -> StringType,
      "Claim_Type" -> StringType,
      "FailCode_2" -> StringType,
      "FailCode_2_Description" -> StringType,
      "FailCode_3" -> StringType,
      "FailCode_3_Description" -> StringType,
      "Part_Number_Original" -> StringType,
      "Failure_Part_Original" -> StringType,
      "Cause_Part" -> StringType,
      "Travel_Costs" -> StringType,
      "Management_Costs" -> StringType,
      "Other_Costs" -> StringType,
      "Undetail_Cost" -> StringType,
      "Mark_Up_Costs" -> StringType,
      "BFC_Costs" -> StringType,
      "Tax" -> StringType,
      "Overseas_Total_Claim_Cost" -> StringType,
      "NWG_Management_Cost" -> StringType,
      "Brand_Name" -> StringType,
      "Vehicle_Number" -> StringType,
      "Customer_Name" -> StringType,
      "Customer_Cell" -> StringType,
      "Customer_Company" -> StringType,
      "Responsibility" -> StringType,
      "CES" -> StringType,
      "Need_Deduct" -> StringType,
      "Old_Parts_Returns" -> StringType,
      "Failure_Mode" -> StringType,
      "Cummins_IntegerTypeernal_Supplier" -> StringType,
      "Sub_system" -> StringType,
      "Distributor_or_not" -> StringType,
      "Wrok_Order_Number" -> StringType,
      "DB" -> StringType,
      "Engine_Platform" -> StringType,
      "Part_warranty_start_date" -> StringType,
      "Hours_after_installation_of_parts" -> StringType,
      "Mileage_after_installation_of_parts" -> StringType,
      "User_address" -> StringType,
      "Total_Submitted_Travel_Cost" -> StringType,
      "Total_Submitted_Other_Cost" -> StringType,
      "Reporter" -> StringType,
      "Service_expert_reviewer" -> StringType,
      "Service_expert_review_time" -> StringType,
      "Total_Pre_audit_Material_Cost" -> StringType,
      "Total_Pre_audit_Labor_Cost" -> StringType,
      "Total__Pre_audit_Warranty_Cost" -> StringType,
      "Final_reviewer" -> StringType,
      "Final_date" -> StringType,
      "Plant_ID_Code" -> StringType,
      "FAM" -> StringType,
      "Design_Phase_Code" -> StringType,
      "VAR" -> StringType,
      "Horsepower" -> StringType,
      "MKTG_RPM" -> StringType,
      "Engine_Ship_Date" -> StringType,
      "MIS" -> StringType,
      "Origunit" -> StringType,
      "Distributor_Code" -> StringType,
      "CLYR" -> StringType,
      "CLAIM_NUM" -> StringType,
      "ACCT" -> StringType,
      "Markup_Amount" -> StringType,
      "CPL" -> StringType,
      "Coverage" -> StringType,
      "Time_Lag_From_Failure_To_Submit" -> StringType,
      "ABO_Failure_Part" -> StringType,
      "Fail_Code_Mode" -> StringType,
      "Total_Cost_USD" -> StringType,
      "Fault_Code" -> StringType,
      "Failure_Part_Name_Original" -> StringType,
      "PA" -> StringType,
      "Job_object" -> StringType,
      "working_environment" -> StringType,
      "Engine_Family" -> StringType,
      "Total_Audit_Cause_Part_Cost" -> StringType,
      "Correction_2" -> StringType,
      "Correction_3" -> StringType,
      "Non_Cause_Part" -> StringType,
      "Total_Audit_Non_Cause_Part_Cost" -> StringType,
      "Data_Provider" -> StringType,
      "Paid_Open" -> StringType,
      "EW_PART_OR_NOT_NS_VI_ONLY" -> StringType,
      "Year" -> StringType
    )
    val strSql: String =
      s"""
         |SELECT
         |`鉴定单号_claim_number` as claim_number,
         |`实体索赔性质_coverage_entity` as coverage_entity,
         |`保修性质_coverage_program` as coverage_program,
         |`整机索赔标志_whole_engine_claim_or_not` as whole_engine_claim_or_not,
         |case when length(`填单日期_claim_submit_date`)!=0 then `填单日期_claim_submit_date` else '1900-01-01' end as claim_submit_date,
         |`互调件标志` as change_part_flag,
         |`对方鉴定单号_third_party_claim_id` as third_party_claim_id,
         |`审核标志_assessment_progress` as assessment_progress,
         |`服务站名称_dealer_name` as dealer_name,
         |`服务站鉴定章号_dealer_code` as dealer_code,
         |`发动机号_esn` as esn,
         |`发动机号_esn_new` as esn_new,
         |`订单号_so_` as so,
         |`发动机功率_engine_power` as engine_power,
         |`型号描述_engine_type` as engine_type,
         |case when length(`生产日期_build_date`)!=0 then `生产日期_build_date` else '1900-01-01' end as build_date,
         |`发动机应用领域_userapp` as userapp,
         |`行驶里程_运行里程或工作小时_mileage_or_hour` as mileage_or_hour,
         |`oem代码_oem_code` as oem_code,
         |`oem名称_oem_name` as oem_name,
         |`车型_vehicle_model` as vehicle_model,
         |`底盘号_chassis_number` as chassis_number,
         |case when length(`整车生产日期_vehicle_build_date`)!=0 then `整车生产日期_vehicle_build_date` else '1900-01-01' end as vehicle_build_date,
         |case when length(`购车日期_vehicle_purchase_date`)!=0 then `购车日期_vehicle_purchase_date` else '1900-01-01' end as vehicle_purchase_date,
         |case when length(`故障时间_failure_date`)!=0 then `故障时间_failure_date` else '1900-01-01' end as failure_date,
         |`故障编码_fail_code` as fail_code,
         |`故障编码说明_fail_code_description` as fail_code_description,
         |`客户抱怨_complaint` as Complaint,
         |`故障分析_failure_cause` as Failure_Cause,
         |`处理措施_correction` as Correction,
         |`责任部门_accountable_dept_` as Accountable_Dept,
         |`故障零件号_failure_part_number` as Failure_Part_Number,
         |`故障零件名称_failure_part` as Failure_Part,
         |`故障件供应商代码_failure_part_supplier_code` as Failure_Part_Supplier_Code,
         |`故障件供应商名称_failure_part_supplier` as Failure_Part_Supplier,
         |`救急合计费用_total_emergent_cost` as Total_Emergent_Cost,
         |`救急辅料费_emergent_material_cost` as Emergent_Material_Cost,
         |`审核救急辅料费_emergent_material_cost_audit` as Emergent_Material_Cost_Audit,
         |`劳务费合计_total_labor_cost` as Total_Labor_Cost,
         |`审核劳务费合计_total_labor_cost_audit` as Total_Labor_Cost_Audit,
         |`工时_labor_hour` as Labor_Hour,
         |`申报材料费总计_total_submitted_material_cost` as Total_Submitted_Material_Cost,
         |`审核材料费总计_total_audit_material_cost` as Total_Audit_Material_Cost,
         |`申报保修费用总计_total_submitted_warranty_cost` as Total__Submitted_Warranty_Cost,
         |`审核保修费用总计_total_audit_warranty_cost` as Total_Audit_Warranty_Cost,
         |`结算审核状态_audit_status` as Audit_Status,
         |case when length(`结算日期_pay_date`)!=0  then `结算日期_pay_date` else '1900-01-01' end as Pay_Date,
         |case when length(`提交财务日期_submit_to_finance_date`)!=0 then `提交财务日期_submit_to_finance_date` else '1900-01-01' end as Submit_to_Finance_Date,
         |`最终合计费用_total_cost` as Total_Cost,
         |`鉴定单渠道_claim_channel` as Claim_Channel,
         |`结算_Pay_Month` as Pay_Month,
         |`年度_Pay_Year` as Pay_Year,
         |`审核救急合计费用_Audit_Emergent_Total_Cost` as Audit_Emergent_Total_Cost,
         |`区域_Region` as Region,
         |`省份_Province` as Province,
         |`批次号_Batch_Number` as Batch_Number,
         |`申报工时费总计_Total_Submitted_Labor_Cost` as Total_Submitted_Labor_Cost,
         |`审核工时费合计_Total_Audit_Labor_Cost` as Total_Audit_Labor_Cost,
         |`材料费_不含管理费_Material_Cost_Without_Management_Fee` as Material_Cost_Without_Management_Fee,
         |`越权审批类型_Audit_Type` as Audit_Type,
         |`后处理采购形式_Aftertreament_Purchasing_Type` as Aftertreament_Purchasing_Type,
         |`ENTITY_CODE` as ENTITY_CODE,
         |`本体或后处理_Base_or_ATS` as Base_or_ATS,
         |`序号_No` as No,
         |`排量_Displacement` as Displacement,
         |`排放_Emission_Level` as Emission_Level,
         |`后处理类型_ATS_Type` as ATS_Type,
         |`东风_非东风_DONGFENG_NONE_DONGFENG` AS DONGFENG_NONE_DONGFENG,
         |`UA2_Blockage` as UA2_Blockage,
         |`FC2772` as FC2772,
         |`旧件检测_Returned_Part_Analysis` as Returned_Part_Analysis,
         |`MILEAGE_DISTRIBUTION_KEY` as MILEAGE_DISTRIBUTION_KEY,
         |`HOUR_DISTRIBUTION_KEY` as HOUR_DISTRIBUTION_KEY,
         |`售前_售后标志` as BIS_AIS,
         |`属性` as attribute,
         |`保修起始日_In_Service_Date` as In_Service_Date,
         |`细分市场_Vehicle_Type` as Vehicle_Type,
         |`设备型号` as device_model,
         |`失效里程_Mileage` as Mileage,
         |`终审差旅总额` as Final_travel_amount_total,
         |`终审其他总额` as Final_other_amount_total,
         |`扣款总额` as Total_deduction,
         |`扣款原因` as Reason_for_deduction,
         |`申报备注` as Declaration_note,
         |`初审备注` as Initial_review_notes,
         |`终审备注` as Final_review_note,
         |`退单备注` as Rebate_note,
         |`提交次数` as Number_of_submissions,
         |`零件状态` as Part_status,
         |`SO状态` as SO_status,
         |`旧流水号` as Old_flow_number,
         |`新流水号` as New_flow_number,
         |`发票费用合计_Invoice_Total_AMT` as Invoice_Total_AMT,
         |`OEM_零件号_Parts_NO_` as OEM_Parts_NO,
         |`CES零件号_CES_Parts_NO_` as CES_Parts_NO,
         |`用户_Customer` as Customer,
         |`索赔合理性_Claim_Reasonable_or_Not` as Claim_Reasonable_or_Not,
         |`索赔不合理情况_Claim_Abnormal_Comments` as Claim_Abnormal_Comments,
         |`申诉与否_Apeal_or_Not` as Apeal_or_Not,
         |`申诉原因_Appeal_Reason` as Appeal_Reason,
         |`申诉结果_Appeal_Result` as Appeal_Result,
         |`成功申诉金额_Appeal_Success_AMT_Without_Tax` as Appeal_Success_AMT_Without_Tax,
         |`首要故障模式_Prime_Failure_Mode` as Prime_Failure_Mode,
         |`是否是CES责任_NO_CES_Responsibiliy` as NO_CES_Responsibiliy,
         |`付款差额_Payment_Difference` as Payment_Difference,
         |`Policy_in_BFDA` as Policy_in_BFDA,
         |`关键OEM名称_Key_OEM` as Key_OEM,
         |`IPTV3` as IPTV3,
         |`零件生产日期_Parts_Production_Date` as Parts_Production_Date,
         |`零件序列号_Part_SN` as Part_SN,
         |`失效数量_Failure_Quantity` as Failure_Quantity,
         |`Description_level` as Description_level,
         |`MIB_Day` as MIB_Day,
         |`MIB_Month` as MIB_Month,
         |`含税前置费用合计_Total_AMT` as Total_AMT,
         |`含税申诉成功金额_Appeal_Success_AMT_With_Tax` as Appeal_Success_AMT_With_Tax,
         |`PR_No_` as PR_No,
         |`申诉单号_Appeal_No_` as Appeal_No,
         |`镜像_Image` as Image,
         |`索赔转嫁月份_Warranty_Mon` as Warranty_Mon,
         |`用户意见` as User_opinion,
         |`用户姓名` as username,
         |`用户电话` as User_phone,
         |`用户单位` as User_units,
         |`新零件已运行小时` as New_part_has_been_running_hours,
         |`新零件已行驶里程` as Mileage_of_new_parts,
         |`新零件保修起始日` as New_part_warranty_start_date,
         |`维修地点` as Repair_location,
         |`维修次数` as Number_of_repairs,
         |`损坏程度` as level_of_damage,
         |`首次故障运行小时` as First_failure_hour,
         |`首次故障行驶里程` as First_fault_mileage,
         |`手机` as Mobile_phone,
         |`审核备注` as Audit_note,
         |`设备名称` as Device_name,
         |`结算批次` as Settlement_batch,
         |`故障零件英文名称_Failure_Part` as Failure_Part_English,
         |`失效小时_Work_Hour` as Work_Hour,
         |`工作档案号` as Job_file_number,
         |`工程师审核备注` as Engineer_review_notes,
         |`付款日期` as Payment_date,
         |`服务人员` as service_personnel,
         |`服务开始时间` as Service_start_time,
         |`服务结束时间` as Service_end_time,
         |`服务电话` as service_line,
         |`待转嫁金额` as Amount_to_be_transferred,
         |`CAC初审退单次数` as CAC_initial_review_return_number,
         |`CAC工程师审核人员` as CAC_Engineer_Reviewer,
         |`CAC工程师审核时间` as CAC_Engineer_Review_Time,
         |`CAC工程师退单备注` as CAC_Engineer_Retirement_Notes,
         |`CAC工程师退单次数` as CAC_engineer_refunds,
         |`CAC工程师退单时间` as CAC_engineer_return_time,
         |`初审差旅费` as Initial_review_travel_expenses,
         |`初审零件费` as Initial_review_part_fee,
         |`初审其他费` as Initial_review_other_fees,
         |`初审人员` as First_instance,
         |`初审通过时间` as First_trial_pass_time,
         |`初审退单时间` as Initial_review_time,
         |`倒扣管理费` as Reverse_management_fee,
         |`倒扣审核意见` as Inverted_review_comments,
         |`倒扣状态` as Inverted_state,
         |`旧零件保修起始日` as Old_parts_warranty_start_date,
         |`旧零件已行驶里程` as Old_parts_mileage,
         |`旧零件已运行小时` as Old_parts_have_been_running_hours,
         |`录入差旅费` as Entering_travel_expenses,
         |`录入工时费` as Entering_working_hours,
         |`录入其他费用` as Enter_other_fees,
         |`终审通过时间` as Final_approval_time,
         |`终审退单备注` as Final_review_reimbursement_note,
         |`终审退单次数` as Final_review_order_number,
         |`终审退单时间` as Final_review_time,
         |`数据时间_Data_Month` as Data_Month,
         |`国内国外_Dom_or_Int` as Dom_or_Int,
         |`索赔类型_Claim_Type` as Claim_Type,
         |`故障代码_FailCode_2` as FailCode_2,
         |`故障代码描述_FailCode_2_Description` as FailCode_2_Description,
         |`故障代码_FailCode_3` as FailCode_3,
         |`故障代码描述3_FailCode_3_Description` as FailCode_3_Description,
         |`零件号_原_Part_Number_Original` as Part_Number_Original,
         |`失效件_原_Failure_Part_Original` as Failure_Part_Original,
         |`祸首件_英_Cause_Part` as Cause_Part,
         |`差旅费_Travel_Costs` as Travel_Costs,
         |`管理费_Management_Costs` as Management_Costs,
         |`其它费用_Other_Costs` as Other_Costs,
         |`不明费用_Undetail_Cost` as Undetail_Cost,
         |`溢价费用_Mark_Up_Costs` as Mark_Up_Costs,
         |`转运成本_BFC_Costs` as BFC_Costs,
         |`税费_Tax` as Tax,
         |`海外总费用_Overseas_Total_Claim_Cost` as Overseas_Total_Claim_Cost,
         |`CCI服务费_NWG_Management_Cost` as NWG_Management_Cost,
         |`品牌名称_Brand_Name` as Brand_Name,
         |`出厂编码_Vehicle_Number` as Vehicle_Number,
         |`客户姓名_Customer_Name` as Customer_Name,
         |`客户电话_Customer_Cell` as Customer_Cell,
         |`客户地址_Customer_Company` as Customer_Company,
         |`责任判定_Responsibility` as Responsibility,
         |`CES` as CES,
         |`Need_Deduct` as Need_Deduct,
         |`旧件返回情况_Old_Parts_Returns` as Old_Parts_Returns,
         |`故障模式_Confirm_Failure_Mode` as Failure_Mode,
         |`康明斯内部供应商_Cummins_Internal_Supplier` as Cummins_Internal_Supplier,
         |`子系统_Sub_system` as Sub_system,
         |`是否经销商` as Distributor_or_not,
         |`工作单号_Wrok_Order_Number` as Wrok_Order_Number,
         |`分公司名称_DB` as DB,
         |`发动机平台_Engine_Platform` as Engine_Platform,
         |`零件保修起始日期` as Part_warranty_start_date,
         |`零件安装后运行小时` as Hours_after_installation_of_parts,
         |`零件安装后运行里程` as Mileage_after_installation_of_parts,
         |`用户地址` as User_address,
         |`申报差旅价_Total_Submitted_Travel_Cost` as Total_Submitted_Travel_Cost,
         |`申报其他价_Total_Submitted_Other_Cost` as Total_Submitted_Other_Cost,
         |`申报人员` as Reporter,
         |`服务专家审核人` as Service_expert_reviewer,
         |`服务专家审核通过时间` as Service_expert_review_time,
         |`初审审核材料费总计_Total_Pre_audit_Material_Cost` as Total_Pre_audit_Material_Cost,
         |`初审审核工时费合计 Total_Pre_audit_Labor_Cost` as Total_Pre_audit_Labor_Cost,
         |`初审保修费用总计_Total_Pre_audit_Warranty_Cost` as Total__Pre_audit_Warranty_Cost,
         |`终审人员` as Final_reviewer,
         |`终审日期` as Final_date,
         |`工厂代码_Plant_ID_Code` as Plant_ID_Code,
         |`FAM` as FAM,
         |`设计阶段代码_Design_Phase_Code` as Design_Phase_Code,
         |`VAR` as VAR,
         |`马力_Horsepower` as Horsepower,
         |`转速_MKTG_RPM` as MKTG_RPM,
         |`发动机发运日期_Engine_Ship_Date` as Engine_Ship_Date,
         |`MIS` as MIS,
         |`原始索赔数据里程单位_Origunit` as Origunit,
         |`经销商代码Distributor_Code` as Distributor_Code,
         |`CLYR` as CLYR,
         |`CLAIM_NUM` as CLAIM_NUM,
         |`索赔性质代码_ACCT` as ACCT,
         |`零件加价_Markup_Amount` as Markup_Amount,
         |`CPL` as CPL,
         |`ABO索赔性质_Coverage` as Coverage,
         |`时间间隔_失效到提交索赔_Time_Lag_From_Failure_To_Submit` as Time_Lag_From_Failure_To_Submit,
         |`ABO统一故障零件名称_ABO_Failure_Part` as ABO_Failure_Part,
         |`故障编码及模式_Fail_Code_Mode` as Fail_Code_Mode,
         |`最终合计费用美元_Total_Cost_USD` as Total_Cost_USD,
         |`故障码_Fault_Code` as Fault_Code,
         |`原始故障零件名称_Failure_Part_Name_Original` as Failure_Part_Name_Original,
         |`PA` as PA,
         |`作业对象` as Job_object,
         |`工作环境` as working_environment,
         |`发动机系列_Engine_Family` as Engine_Family,
         |`终审元凶件零件费用_Total_Audit_Cause_Part_Cost` as Total_Audit_Cause_Part_Cost,
         |`处理措施_Correction_2` as Correction_2,
         |`处理措施_Correction_3` as Correction_3,
         |`非元凶件名称_Non_Cause_Part` as Non_Cause_Part,
         |`终审非元凶件零件费用_Total_Audit_Non_Cause_Part_Cost` as Total_Audit_Non_Cause_Part_Cost,
         |`数据提供单位_Data_Provider` as Data_Provider,
         |`结算未结算_Paid_Open` as Paid_Open,
         |`是否排放质保零件ew_part_or_not_ns_vi_only` as EW_PART_OR_NOT_NS_VI_ONLY,
         |`Year` as Year
         |FROM ${cfg(Constants.RAW_DB)}.${cfg(Constants.RAW_REL_CLM_TBL)}
    """.stripMargin
    Util.applySchema(spark.sql(strSql), RelEngineClaimDetailSchema)
  }
}
