package mck.qb.columbia.modelInput.uv

import mck.qb.columbia.constants.Constants

object DetectMasterFJob extends DetectMasterJob {
  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)

  override def getEngType: String = "F"
}
