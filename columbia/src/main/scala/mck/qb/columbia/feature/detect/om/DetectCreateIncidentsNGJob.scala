package mck.qb.columbia.feature.detect.om

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.feature.detect.om.base.DetectCreateIncidentsJob

object DetectCreateIncidentsNGJob extends DetectCreateIncidentsJob {
  override def getEngType(): String = "NG"

  override def getTargetTableKey(): String = Constants.ALL_INC_NG_TBL
}
