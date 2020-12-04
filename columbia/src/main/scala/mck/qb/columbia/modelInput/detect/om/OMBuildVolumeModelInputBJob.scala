package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMBuildVolumeModelInputBJob extends OMBuildVolumeModelInputJob {
  override def getEngType: String = "B"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
