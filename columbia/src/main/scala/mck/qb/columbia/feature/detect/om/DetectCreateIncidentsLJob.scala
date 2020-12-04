package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsLJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "L"

  override def getTargetTableKey(): String = Constants.ALL_INC_L_TBL
}
