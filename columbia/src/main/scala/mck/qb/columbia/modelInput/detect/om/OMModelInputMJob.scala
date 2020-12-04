package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMModelInputMJob extends OMModelInputJob{

  override def getEngType: String = "M"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
