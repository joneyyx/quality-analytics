package mck.qb.columbia.config

import com.typesafe.config.{Config, ConfigFactory}
import mck.qb.columbia.constants.Constants

object AppConfig extends Serializable {

  class AppConfigException( s : String ) extends Exception( s ) {}

  private var objConfig: Config = _

  private def initialize(): Unit = {
    this.objConfig = ConfigFactory.load().
      withFallback(ConfigFactory.parseString(_defaultConfig()))
  }

  def getMyConfig(sPipeline: String): Config = try {
    // Create a lock on here and check to initialize
    if(this.objConfig == null) this.initialize()

    this.objConfig.getConfig(sPipeline).withFallback(this.objConfig.getConfig("COMMON"))
  }
  catch {
    case e: Exception =>
      e.printStackTrace()
      throw new AppConfigException(e.getMessage)
  }

  def getMyConfig(confPrefix: String, logger: org.apache.log4j.Logger): Config = {
    val myConf = AppConfig.getMyConfig(confPrefix)
    logger.warn("Config utilized - (%s)".format(myConf))
    myConf
  }

  private def _defaultConfig():String = {

      s"""
         |
         |${Constants.PRI_REL_ENG_CLM}.CN=mck.qb.columbia.primary.reliability.RelEngineClaimsPrimaryJob
         |${Constants.PRI_REL_ENG_DTL}.CN=mck.qb.columbia.primary.reliability.RelEngineDetailsPrimaryJob
         |${Constants.PRI_TEL_FC_DTL}.CN=mck.qb.columbia.primary.telematics.TelFaultCodePrimaryJob
         |
         |${Constants.REF_DETECT_CRT}.CN=mck.qb.columbia.reference.CreateReferenceJob
         |
         |${Constants.FET_REL_ENG_CLM}.CN=mck.qb.columbia.feature.detect.reliability.RelEngineClaimFeaturesJob
         |${Constants.FET_REL_ENG_DTL}.CN=mck.qb.columbia.feature.detect.reliability.RelEngineDetailsFeaturesJob
         |${Constants.FET_TEL_FC_DTL}.CN=mck.qb.columbia.feature.detect.telematics.TelFaultCodeFeaturesJob
         |${Constants.FET_DETECT_OM_REL}.CN=mck.qb.columbia.feature.detect.om.OccurrenceMonitoringRelFeatureJob
         |${Constants.FET_DETECT_OM_TEL}.CN=mck.qb.columbia.feature.detect.om.OccurrenceMonitoringTELFeatureJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsBJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsDJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsFJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsLJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsMJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsNGJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsXJob
         |${Constants.FET_DETECT_CRT_INC}.CN=mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsZJob
         |
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputBJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputDJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputFJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputLJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputMJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputNGJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputXJob
         |${Constants.MIP_DETECT_OM}.CN=mck.qb.columbia.modelInput.detect.om.OMModelInputZJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputBJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputDJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputFJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputLJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputMJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputNGJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputXJob
         |${Constants.MIP_DETECT_OM_BV}.CN=mck.qb.columbia.modelInput.detect.om.OMBuildVolumeModelInputZJob
         |
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterBJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterDJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterFJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterLJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterMJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterNGJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterXJob
         |${Constants.MIP_UV_MST}.CN=mck.qb.columbia.modelInput.uv.DetectMasterZJob
         |
         |${Constants.RPT_RPT_OM_RPT}.CN=mck.qb.columbia.report.OccurenceMonitoringReportingJob
				 |
				 |${Constants.MTL_DATA_QLT}.CN=mck.qb.columbia.monitoring.MonitoringDataQualityJob
         |
      """.stripMargin
  }
}
