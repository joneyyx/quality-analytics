package mck.qb.columbia.primary.ssap

import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object TelCsuTrim extends Job {
//	val myConfig = AppConfig.getMyConfig(Constants.PRI_SSAP_TRIM_DTL)
//	logger.warn(s"xxx".format(myConfig))
//	val cfg: String => String = myConfig.getString

	override def run(): Unit = {
		val trim = spark.table("raw_tel.tel_csu_trim_data")
		val esnmaster = spark.read.orc("wasbs://container-dl-dev-qadstest-spark@dldevblob4hdistorage.blob.core.chinacloudapi.cn/pri_tel/esn_master_detail/")
		val testJoin = trim.join(esnmaster, Seq("esn"), "left_outer")

//		val hivePath = "wasbs://container-dl-dev-qadstest-spark@dldevblob4hdistorage.blob.core.chinacloudapi.cn/hive/warehouse/"

//		Util.saveDataWithPartition(testJoin, "pri_tel", "tel_csu_trim_changed_1", Array("partition", "report_date"))

	}
}

