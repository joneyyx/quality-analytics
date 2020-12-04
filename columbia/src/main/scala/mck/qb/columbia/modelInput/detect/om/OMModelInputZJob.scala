package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMModelInputZJob extends OMModelInputJob{

  override def getEngType: String = "Z"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
