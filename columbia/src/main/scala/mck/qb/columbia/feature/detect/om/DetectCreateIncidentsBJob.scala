package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsBJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "B"

  override def getTargetTableKey(): String = Constants.ALL_INC_B_TBL
}
