package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsDJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "D"

  override def getTargetTableKey(): String = Constants.ALL_INC_D_TBL
}
