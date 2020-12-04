package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsFJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "F"

  override def getTargetTableKey(): String = Constants.ALL_INC_F_TBL
}
