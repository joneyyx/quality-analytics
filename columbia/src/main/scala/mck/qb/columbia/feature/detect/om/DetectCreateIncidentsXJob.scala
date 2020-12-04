package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsXJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "X"

  override def getTargetTableKey(): String = Constants.ALL_INC_X_TBL
}
