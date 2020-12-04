package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMModelInputXJob extends OMModelInputJob{

  override def getEngType: String = "X"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
