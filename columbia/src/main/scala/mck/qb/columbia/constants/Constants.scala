package mck.qb.columbia.constants

object Constants {

  // Common
  val PATH                    = "path"
  val ENV                     = "ENV"
  val HDFS_PATH               = "FS.HDFS_PATH"
  val FIRST_TIME_RUN          = "FT_RUN"
  val DATA_SAMPLE             = "DATA_SMP"
  val EFPA_HIST_START         = "START_TIME"
  val EFPA_HIST_END           = "END_TIME"

  /*
    * Pipeline JOB Keys
    */



  // Raw

  // Primary
  val PRI_REL_ENG_CLM       = "PRI.REL.ENG_CLM"
  val PRI_REL_ENG_DTL       = "PRI.REL.ENG_DTL"
  val PRI_TEL_FC_DTL        = "PRI.TEL.FC_DTL"
  val PRI_JIRA_ISUS         = "PRI.JIRA.ISUS"


  // Features
  val FET_REL_ENG_CLM     = "FET.REL.ENG_CLM"
  val FET_REL_ENG_DTL     = "FET.REL.ENG_DTL"
  val FET_TEL_FC_DTL      = "FET.TEL.FC_DTL"
  val FET_DETECT_OM_REL   = "FET.DETECT.OM_REL"
  val FET_DETECT_OM_TEL   = "FET.DETECT.OM_TEL"
  val FET_DETECT_CRT_INC  = "FET.DETECT.CRT_INC"

  // ModelInput -- JOBS
  val MIP_DETECT_OM       = "MIP.DETECT.OM"
  val MIP_DETECT_OM_BV    = "MIP.DETECT.OM_BV"
  val MIP_UV_MST          = "MIP.UV.DM"
  var MIP_WB_FET          = "MIP.WB.FET"

  // ModelOutput -- JOBS

  //Reference -- JOBS
  val REF_DETECT_CRT = "REF.DETECT.CRT"


  // Reporting -- JOBS
  val RPT_RPT_OM_RPT        = "RPT.RPT.OM_RPT"
  val RPT_RPT_UV            = "RPT.RPT.UV"
  val RPT_RPT_UV_RVC        = "RPT.RPT.UV_RVC"
  val RPT_RPT_AR_DIM      = "RPT.RPT.AR_DIM"
  val RPT_RPT_WB          = "RPT.RPT.WB"

  //Export -- JOBS
  val EXP_REL_BUILD = "EXP.REL.BUILD"
  val EXP_BUILD_IFNC = "EXP.BUILD.IFNC"
  val EXP_BUILD_EW   = "EXP.BUILD.EW"

  //Monitoring
  val MTL_DATA_QLT           = "MTL.PRI.MON_DATA_QLT"

  /**
    * Database keys
    */

  // Default
  val REF_DB              = "DB.REF"
  val RAW_DB              = "DB.RAW"
  val PRI_DB              = "DB.PRIMARY"
  val FET_DB              = "DB.FEATURE"
  val MODEL_IP_DB         = "DB.MODEL_IP"
  val MODEL_OP_DB         = "DB.MODEL_OP"
  val REPORT_DB           = "DB.REPORT"
  val REPORT_SQL_DB       = "DB.REPORT_SQL_DB"
  val EXPORT_DB           = "DB.EXPORT"
  val TRIM_DB             = "DB.TRIM"
  val PRI_TEL_DB          = "DB.PRI_TEL"
  val MTL_DB              = "DB.PRI_MTL"


  /**
    * Table keys
    */

  // Default
  val REF_TBL             = "TN.REF_TBL"
  val RAW_TBL             = "TN.RAW_TBL"
  val PRI_TBL             = "TN.PRI_TBL"
  val FET_TBL             = "TN.FET_TBL"
  val MODEL_IP_TBL        = "TN.MIP_TBL"
  val MODEL_OP_TBL        = "TN.MOP_TBL"
  val REPORT_TBL          = "TN.RPT_TBL"

  //JIRA
  val JIRA_ISUS_RAW_TBL = "TN.JIRA_ISUS_RAW"
  val JIRA_ISUS_DTL_TBL = "TN.JIRA_ISUS_DTL"

  // RELIABILITY
  val RAW_REL_BLD_TBL           = "TN.RAW_REL_BLD"
  val RAW_REL_CLM_TBL           = "TN.RAW_REL_CLM"
  val REL_ENG_DTL_TBL           = "TN.REL_ENG_DTL"
  val REL_ENG_CLM_DTL_TBL       = "TN.REL_ENG_CLM_DTL"
  val REL_ENG_FET_TBL           = "TN.REL_ENG_FET"
  val REL_CLM_FET_TBL           = "TN.REL_CLM_FET"
  val REL_OEM_CHASSIS_TBL       = "TN.REL_OEM_CHASSIS"
  val REL_OEM_CHASSIS_NS_TBL    = "TN.REL_OEM_CHASSIS_NS"



  // Telematics
  val RAW_TEL_TBL               = "TN.RAW_TEL"
  val RAW_TEL_ERR_TBL           = "TN.RAW_TEL_ERR"
  val TEL_FC_DTL_TBL            = "TN.TEL_FC_DTL"
  val TEL_FNL_FET_TBL           = "TN.TEL_FNL_FET"
  val ESN_MST_DTL_TBL           = "TN.ESN_MST_DTL"

  // Incident
  val ALL_INC_TBL                 = "TN.ALL_INC"
  val ALL_INC_X_TBL               = "TN.ALL_INC_X"
  val ALL_INC_B_TBL               = "TN.ALL_INC_B"
  val ALL_INC_F_TBL               = "TN.ALL_INC_F"
  val ALL_INC_D_TBL               = "TN.ALL_INC_D"
  val ALL_INC_L_TBL               = "TN.ALL_INC_L"
  val ALL_INC_NG_TBL              = "TN.ALL_INC_NG"
  val ALL_INC_M_TBL               = "TN.ALL_INC_M"
  val ALL_INC_Z_TBL               = "TN.ALL_INC_Z"

  // OccurenceMonitoring
  val OCM_REL_MTR_TBL               = "TN.OCM_REL_MTR"
  val OCM_REL_MTR_WARRANTY_TBL      = "TN.OCM_REL_MTR_WARRANTY"
  val OCM_REL_MTR_POLICY_TBL        = "TN.OCM_REL_MTR_POLICY"
  val OCM_REL_MTR_CAMPAIGN_TRP_TBL  = "TN.OCM_REL_MTR_CAMPAIGN_TRP"
  val OCM_REL_MTR_FIELD_TEST_TBL    = "TN.OCM_REL_MTR_FIELD_TEST"
  val OCM_REL_MTR_EMISSION_TBL      = "TN.OCM_REL_MTR_EMISSION"
  val OCM_REL_MTR_ALL_TBL           = "TN.OCM_REL_MTR_ALL"
  val OCM_TEL_MTR_TBL               = "TN.OCM_TEL_MTR"
  val OM_BV_RESULT                  = "TN.OM_BV_RES"
  val OM_OUTPUT_TBL                 = "TN.OM_OUTPUT"
  val OM_STATS_TBL                  = "TN.OM_STATS"
  val OM_HL_FAU_CODE_TBL             = "TN.OM_HL_FAU_CODE"
  val OCM_TEL_MTR_FLD_TBL           = "TN.OCM_TEL_MTR_FLD"
  val OCM_TEL_MTR_NON_FLD_TBL       = "TN.OCM_TEL_MTR_NON_FLD"

  // Reference
  val REF_TFC_DESC_TBL      = "TN.REF_TFC_DESC"
  val REF_FP_DESC_TBL       = "TN.REF_FP_DESC"
  val REF_PER_INCS_TBL      = "TN.REF_PER_INCS"
  val REF_DLR_LOC_TBL       = "TN.REF_DLR_LOC"
  val REF_HL_CODE_TBL       = "TN.REF_HL_CODE"
  val REF_FST_CONT_TBL      = "TN.REF_FST_CONT"
  val REF_NSV_ENG_TBL       = "TN.REF_NSV_ENG"
  val REF_NSVI_ENG_TBL      = "TN.REF_NSVI_ENG"
  val REF_TEL_FT_ESN_TBL    = "TN.REF_TEL_FIELD_TEST_ESN"
  val REF_PRT_TYPE_MP_TBL   = "TN.REF_PRT_TYPE_MP"
  val REF_PRT_TYPE_WRT_TBL  = "TN.REF_PRT_TYPE_WRT"
  val REF_NSVI_FAU_COD_REA_DESC_TBL = "TN.REF_NSVI_FAU_COD_REA_DESC"
  val REF_NSV_FAU_COD_REA_DESC_TBL  = "TN.REF_NSV_FAU_COD_REA_DESC"
  val REF_HB_AGG_INFO_TBL           = "TN.REF_HB_AGG_INFO"
  val REF_NSVI_EW_FP_TBL = "TN.REF_NSVIEW_FP"
  val REF_NSVI_EW_FC_TBL = "TN.REF_NSVIEW_FC"
  val REF_TEL_GDB_TBL               = "TN.REF_TEL_GDB"

  //Universal View
  val FULL_FET_TBL            = "TN.FULL_FET"
  val UV_MASTER_ESN_ALL_TBL   = "TN.UV_MASTER_ESN_ALL"
  val UV_PART_NUMS_TBL        = "TN.UV_PART_NUMS"
  val UV_MASTER_TBL           = "TN.UV_MASTER"
  val UV_CODE_DESCRIPTION_TBL = "TN.UV_CODE_DESC"
  val UV_FAIL_CODE_TBL        = "TN.FAIL_CODE"
  val UV_FAULT_CODE_TBL       = "TN.FAULT_CODE"
  val UV_TC_DATE_TBL          = "TN.UV_TC_DATE"
  val UV_BUL_VOL_TBL          = "TN.UV_BUL_VOL"
  val UV_FAU_CODE_REL_TBL     = "TN.UV_FAU_CODE_REL"
  val UV_FAI_PAR_REL_TBL      = "TN.UV_FAI_PAR_REL"

  //WeiBull
  val MIP_WB_ENG_TBL = "TN.WEIBULL_ENG_FET"
  val MIP_WB_CLM_TBL = "TN.WEIBULL_CLM_FET"
  val MOP_WB_TBL     = "TN.WB_MODEL_OUTPUT"
  val WB_RPT_TBL     = "TN.WB_RPT"

  //DIM
  val REF_ROLES_TBL          = "TN.REF_ROLES"
  val DIM_PERMISSION_TBL         ="TN.DIM_PMS"

  //Export
  val EXP_BLD_HIS_TBL   = "TN.EXP_BLD_HIS"
  val EXP_IFC_BLD_TBL   = "TN.EXP_IFC_BLD"
  val EXP_BLD_HIS_TMP_TBL = "TN.EXP_BLD_HIS_TMP"
  val EXP_BLD_EW_TBL    =  "TN.EXP_BLD_EW"


  //TRIM
  val TEL_CSU_TRIM_TBL      = "TN.TEL_CSU_TRIM"

  //MONITORING
  val QLT_SCO_TBL           = "TN.QLT_SCO"



}
