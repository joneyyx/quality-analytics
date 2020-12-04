package mck.qb.columbia.modelInput.detect.om

import mck.qb.columbia.constants.Constants

object OMModelInputLJob extends OMModelInputJob{

  override def getEngType: String = "L"

  override def getTargetDB: String = cfg(Constants.MODEL_IP_DB)
}
