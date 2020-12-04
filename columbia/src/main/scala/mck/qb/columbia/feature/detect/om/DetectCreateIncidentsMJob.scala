package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsMJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "M"

  override def getTargetTableKey(): String = Constants.ALL_INC_M_TBL
}
