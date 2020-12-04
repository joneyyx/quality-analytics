package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMBuildVolumeModelInputXJob extends OMBuildVolumeModelInputJob {
  override def getEngType: String = "X"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
