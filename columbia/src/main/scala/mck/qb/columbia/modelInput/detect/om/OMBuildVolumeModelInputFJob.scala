package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMBuildVolumeModelInputFJob extends OMBuildVolumeModelInputJob {
  override def getEngType: String = "F"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
