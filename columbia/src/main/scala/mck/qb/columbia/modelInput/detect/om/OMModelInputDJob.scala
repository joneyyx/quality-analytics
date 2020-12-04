package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMModelInputDJob extends OMModelInputJob{

  override def getEngType: String = "D"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
