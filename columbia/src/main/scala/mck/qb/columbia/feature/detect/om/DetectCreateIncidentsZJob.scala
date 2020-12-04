package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsZJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "Z"

  override def getTargetTableKey(): String = Constants.ALL_INC_Z_TBL
}
