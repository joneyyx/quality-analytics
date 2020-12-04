package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMBuildVolumeModelInputNGJob extends OMBuildVolumeModelInputJob {
  override def getEngType: String = "NG"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
