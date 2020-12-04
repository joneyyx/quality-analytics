package mck.qb.columbia.monitoring

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel



object MonitoringDataQualityJob extends Job {
	/**
		* create on 2019/12/24 by zyyx.
		*/

	val myConf: Config = AppConfig.getMyConfig(Constants.MTL_DATA_QLT)
	logger.warn("Config utilized - (%s)".format(myConf))
	val cfg: String => String = myConf.getString

	override def run(): Unit = {

		//1. Calculate the completeness for build data
			val engineFeatures = loadEngineDetail.withColumn("REL_ENGINE_PLATFORM", Util.getNormDesc(col("ENGINE_PLATFORM")))
				.persist(StorageLevel.MEMORY_ONLY)


		//calculate  scorecard of build data, and at last add significance for it
		val engineFeatures_F = engineFeatures.filter("REL_ENGINE_PLATFORM == 'F'").drop("REL_ENGINE_PLATFORM").cache()
		val engineFeatures_X_BFCEC = engineFeatures.filter("REL_ENGINE_PLATFORM == 'X'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'BFCEC'").cache()
		val engineFeatures_X_DCEC = engineFeatures.filter("REL_ENGINE_PLATFORM == 'X'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'DCEC'").cache()
		val engineFeatures_B_ACPL = engineFeatures.filter("REL_ENGINE_PLATFORM == 'B'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'ACPL'").cache()
		val engineFeatures_B_DCEC = engineFeatures.filter("REL_ENGINE_PLATFORM == 'B'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'DCEC'").cache()
		val engineFeatures_D = engineFeatures.filter("REL_ENGINE_PLATFORM == 'D'").drop("REL_ENGINE_PLATFORM").cache()
		val engineFeatures_L = engineFeatures.filter("REL_ENGINE_PLATFORM == 'L'").drop("REL_ENGINE_PLATFORM").cache()
		val engineFeatures_Z = engineFeatures.filter("REL_ENGINE_PLATFORM == 'Z'").drop("REL_ENGINE_PLATFORM").cache()
		val engineFeatures_M = engineFeatures.filter("REL_ENGINE_PLATFORM == 'M'").drop("REL_ENGINE_PLATFORM").cache()

		//		case class tbsTemplate_Build (inTBl: DataFrame, platform: String ,resource: String , plant: String)
		val tbsToDoBuild = Seq(
			tbsTemplate_Build(inTBl = engineFeatures_F, platform = "F", resource = "BUILD", plant = "BFCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_X_BFCEC, platform = "X", resource = "BUILD", plant = "BFCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_X_DCEC, platform = "X", resource = "BUILD", plant = "DCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_B_ACPL, platform = "B", resource = "BUILD", plant = "ACPL"),
			tbsTemplate_Build(inTBl = engineFeatures_B_DCEC, platform = "B", resource = "BUILD", plant = "DCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_D, platform = "D", resource = "BUILD", plant = "DCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_L, platform = "L", resource = "BUILD", plant = "DCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_Z, platform = "Z", resource = "BUILD", plant = "DCEC"),
			tbsTemplate_Build(inTBl = engineFeatures_M, platform = "M", resource = "BUILD", plant = "XCEC")
		)

		val resultOfBuildNSV = runBuild(tbsToDoBuild, "5")
		val resultOfBuildNSVI = runBuild(tbsToDoBuild, "6")

		tbsToDoBuild.foreach(obj => obj.inTBl.unpersist())

		val unionOfBuild = resultOfBuildNSV.union(resultOfBuildNSVI)
			.withColumn("COMPLETENESS", when(col("COMPLETENESS").isNull, lit("NaN")).otherwise(col("COMPLETENESS")))
			.withColumn("VALIDITY", when(col("VALIDITY").isNull, lit("NaN")).otherwise(col("VALIDITY")))


		val buildSign = loadSignOfBuild
		val finalBuilComp = unionOfBuild.join(buildSign, col("COLNAMES") === col("col_Build"), "left_outer").
			select("UPDATE_TIME",
				"RESOURCE",
				"PLATFORM",
				"EMISSION_LEVEL",
				"ENTITY",
				"COLNAMES",
				"SIGNIFICANCE",
				"COMPLETENESS",
				"VALIDITY")

		//save tmp build table to table
		Util.saveData(finalBuilComp, cfg{Constants.MTL_DB}, "build_tmp", cfg{Constants.HDFS_PATH})




		//2. Calculate the completeness for Claims data
		val claimFeatures = loadClaimDetail
		val fillData = engineFeatures.select("ESN", "Emission_Level", "REL_ENGINE_PLATFORM", "Engine_Plant", "Build_Date").withColumnRenamed("ESN", "ESN_BUILD")
		val claimFinanlFeature = fillData.join(claimFeatures, fillData("ESN_BUILD")===claimFeatures("ESN")).drop("ESN_BUILD")
//			.persist(StorageLevel.MEMORY_ONLY)

//		println("claim has number: " + claimFinanlFeature.count())

		val claimFeatures_F = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'F'").drop("REL_ENGINE_PLATFORM").cache()
		val claimFeatures_X_BFCEC = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'X'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'BFCEC'").cache()
		val claimFeatures_X_DCEC = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'X'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'DCEC'").cache()
		val claimFeatures_B_ACPL = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'B'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'ACPL'").cache()
		val claimFeatures_B_DCEC = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'B'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'DCEC'").cache()
		val claimFeatures_D = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'D'").drop("REL_ENGINE_PLATFORM").cache()
		val claimFeatures_L = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'L'").drop("REL_ENGINE_PLATFORM").cache()
		val claimFeatures_Z = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'Z'").drop("REL_ENGINE_PLATFORM").cache()
		val claimFeatures_M = claimFinanlFeature.filter("REL_ENGINE_PLATFORM == 'M'").drop("REL_ENGINE_PLATFORM").cache()


		val tbsToDoClaim = Seq(
			tbsTemplate_Claim(inTBl = claimFeatures_F, platform = "F", resource = "CLAIM", plant = "BFCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_X_BFCEC, platform = "X", resource = "CLAIM", plant = "BFCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_X_DCEC, platform = "X", resource = "CLAIM", plant = "DCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_B_ACPL, platform = "B", resource = "CLAIM", plant = "ACPL"),
			tbsTemplate_Claim(inTBl = claimFeatures_B_DCEC, platform = "B", resource = "CLAIM", plant = "DCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_D, platform = "D", resource = "CLAIM", plant = "DCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_L, platform = "L", resource = "CLAIM", plant = "DCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_Z, platform = "Z", resource = "CLAIM", plant = "DCEC"),
			tbsTemplate_Claim(inTBl = claimFeatures_M, platform = "M", resource = "CLAIM", plant = "XCEC")
		)


		val resultOfClaimNSVPaid = runClaim(tbsToDoClaim, "5", "Paid")
		val resultOfClaimNSVOpen = runClaim(tbsToDoClaim, "5", "Open")
		val resultOfClaimNSVIPaid = runClaim(tbsToDoClaim, "6", "Paid")
		val resultOfClaimNSVIOpen = runClaim(tbsToDoClaim, "6", "Open")

		println("claims are done")

		// unpersit useless RDDs
//		claimFinanlFeature.unpersist()
		tbsToDoClaim.foreach(obj => obj.inTBl.unpersist())

		val unionOfClaim = resultOfClaimNSVPaid.union(resultOfClaimNSVOpen).union(resultOfClaimNSVIPaid).union(resultOfClaimNSVIOpen).
			withColumn("COMPLETENESS", when(col("COMPLETENESS").isNull, lit("NaN")).otherwise(col("COMPLETENESS"))).
			withColumn("VALIDITY", when(col("VALIDITY").isNull, lit("NaN")).otherwise(col("VALIDITY"))).filter("COLNAMES != 'Build_Date'")

		println("union succeeded====== ")

		//add significance for the claim data
		val claimSign = loadSignOfClaim
		val finalClaimComp = unionOfClaim.join(claimSign, col("COLNAMES") === col("col_Claim"), "left_outer").
			select("UPDATE_TIME",
				"RESOURCE",
				"PLATFORM",
				"EMISSION_LEVEL",
				"ENTITY",
				"COLNAMES",
				"SIGNIFICANCE",
				"COMPLETENESS",
				"VALIDITY")

		Util.saveData(finalClaimComp, cfg{Constants.MTL_DB}, "claim_tmp", cfg{Constants.HDFS_PATH})




		//		//3. Calculate the completeness of Telematics
		val telematicsFeatures: Dataset[Row] = loadTelDetail.drop("TELEMATICS_PARTNER_NAME").
			drop("SERVICE_MODEL_NAME").drop("LAMP_COLOR").drop("PRIORITY")

		val fillTelData = engineFeatures.select("ESN", "Emission_Level", "Rel_Engine_Platform", "Engine_Plant", "Build_Date").withColumnRenamed("ESN", "ESN_BUILD")
		val telFinalFeatures = fillTelData.join(telematicsFeatures, fillTelData("ESN_BUILD")===telematicsFeatures("Engine_Serial_Number")).drop("ESN_BUILD").withColumnRenamed("Engine_Serial_Number", "ESN")
//			.persist(StorageLevel.MEMORY_AND_DISK_SER)

		val telFeatures_F = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'F'").drop("REL_ENGINE_PLATFORM").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_X_BFCEC = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'X'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'BFCEC'").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_X_DCEC = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'X'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'DCEC'").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_B_ACPL = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'B'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'ACPL'").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_B_DCEC = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'B'").drop("REL_ENGINE_PLATFORM").filter("ENGINE_PLANT == 'DCEC'").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_D = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'D'").drop("REL_ENGINE_PLATFORM").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_L = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'L'").drop("REL_ENGINE_PLATFORM").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_Z = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'Z'").drop("REL_ENGINE_PLATFORM").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val telFeatures_M = telFinalFeatures.filter("REL_ENGINE_PLATFORM == 'M'").drop("REL_ENGINE_PLATFORM").persist(StorageLevel.MEMORY_AND_DISK_SER)

		val tbsToDoTelematics = Seq(
			tbsTemplate_Telematics(inTBl = telFeatures_F, platform = "F", resource = "TELEMATICS", plant = "BFCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_X_BFCEC, platform = "X", resource = "TELEMATICS", plant = "BFCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_X_DCEC, platform = "X", resource = "TELEMATICS", plant = "DCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_B_ACPL, platform = "B", resource = "TELEMATICS", plant = "ACPL"),
			tbsTemplate_Telematics(inTBl = telFeatures_B_DCEC, platform = "B", resource = "TELEMATICS", plant = "DCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_D, platform = "D", resource = "TELEMATICS", plant = "DCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_L, platform = "L", resource = "TELEMATICS", plant = "DCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_Z, platform = "Z", resource = "TELEMATICS", plant = "DCEC"),
			tbsTemplate_Telematics(inTBl = telFeatures_M, platform = "M", resource = "TELEMATICS", plant = "XCEC")
		)

		val resultOfTelNSV = runTelematics(tbsToDoTelematics, "5")
		val resultOfTelNSVI = runTelematics(tbsToDoTelematics, "6")

		println("telematics finished calculation")
//unpersit useless tables for telematics
		engineFeatures.unpersist()
		tbsToDoTelematics.foreach(obj => obj.inTBl.unpersist())

		val unionOfTel = resultOfTelNSV.union(resultOfTelNSVI).
			withColumn("COMPLETENESS", when(col("COMPLETENESS").isNull, lit("NaN")).otherwise(col("COMPLETENESS"))).
			withColumn("VALIDITY", when(col("VALIDITY").isNull, lit("NaN")).otherwise(col("VALIDITY"))).filter("COLNAMES != 'Build_Date'")

//		add significance for telematics
		val telSign = loadSignOfTel
		val finalTelComp = unionOfTel.join(telSign, col("COLNAMES") === col("col_Telematics"), "left_outer").
			select("UPDATE_TIME",
				"RESOURCE",
				"PLATFORM",
				"EMISSION_LEVEL",
				"ENTITY",
				"COLNAMES",
				"SIGNIFICANCE",
				"COMPLETENESS",
				"VALIDITY")
		Util.saveData(finalTelComp, cfg{Constants.MTL_DB}, "telematics_tmp", cfg{Constants.HDFS_PATH})


		val buildResult = spark.table(cfg{Constants.MTL_DB} + ".build_tmp")
		val claimResult = spark.table(cfg{Constants.MTL_DB} + ".claim_tmp")
		val telResult = spark.table(cfg{Constants.MTL_DB} + ".telematics_tmp")
		val scorecard = buildResult.union(claimResult).union(telResult)
//		Util.saveData(scorecard, cfg(Constants.MTL_DB), cfg(Constants.QLT_SCO_TBL), cfg(Constants.HDFS_PATH))
		Util.saveData(scorecard, cfg(Constants.MTL_DB), cfg(Constants.QLT_SCO_TBL), cfg(Constants.HDFS_PATH), mode = "append")
//			Util.saveData(finalClaimComp, "qa_dev_montoring", "dataquality_scorecard_test_1", cfg(Constants.HDFS_PATH))


	}


	case class tbsTemplate_Build(inTBl: DataFrame, platform: String, resource: String, plant: String)

	def runBuild(tbs: Seq[tbsTemplate_Build], emissionLvl: String): Dataset[Row] = {
		var dfList: Seq[DataFrame] = Seq()
		for (tbDoing <- tbs) {
			val inTBl = tbDoing.inTBl
			val tmp = inTBl.filter(col("Emission_Level").contains(emissionLvl))
			val baseCount: Double = tmp.count()
			val platform = tbDoing.platform
			val resource = tbDoing.resource
			val plant = tbDoing.plant
			val tmpComp: DataFrame = calculateComp(tmp, baseCount, platform, resource, emissionLvl, plant)
			val tmpValid: DataFrame = calValidityBuild(tmp, platform, resource, emissionLvl, plant)
			val result: DataFrame = tmpComp.join(tmpValid, Seq("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES"), "left_outer")
			dfList = dfList :+ result
		}
		dfList.reduce(_.union(_))
	}


	case class tbsTemplate_Claim(inTBl: DataFrame, platform: String, resource: String, plant: String)

	def runClaim(tbs: Seq[tbsTemplate_Claim], emissionLvl: String, claimType: String) = {
		var dfList: Seq[DataFrame] = Seq()
		for (tbDoing <- tbs) {
			val inTBl = tbDoing.inTBl
			val tmp = inTBl.filter(col("Emission_Level").contains(emissionLvl)).filter(col("PAID_OPEN").contains(claimType)).drop("Emission_Level").drop("Engine_Plant")
			val baseCount: Double = tmp.count()
			val platform = tbDoing.platform
			val resource = tbDoing.resource + "_" + claimType
			val plant = tbDoing.plant
			val tmpComp: DataFrame = calculateComp(tmp, baseCount, platform, resource, emissionLvl, plant)
			val tmpValid: DataFrame = calValidityClaim(tmp, platform, resource, emissionLvl, plant)
			val result: DataFrame = tmpComp.join(tmpValid, Seq("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES"), "left_outer")
			dfList = dfList :+ result
		}
		dfList.reduce(_.union(_))
	}

	case class tbsTemplate_Telematics(inTBl: DataFrame, platform: String, resource: String, plant: String)

	def runTelematics(tbs: Seq[tbsTemplate_Telematics], emissionLvl: String) = {
		var dfList: Seq[DataFrame] = Seq()
		for (tbDoing <- tbs) {
			val inTBl = tbDoing.inTBl
			val tmp = inTBl.filter(col("Emission_Level").contains(emissionLvl)).drop("Engine_Plant").drop("Emission_Level")
			val baseCount: Double = tmp.count()
			val platform = tbDoing.platform
			val resource = tbDoing.resource
			val plant = tbDoing.plant
			val tmpComp: DataFrame = calculateComp(tmp, baseCount, platform, resource, emissionLvl, plant)
			val tmpValidity: DataFrame = calValidityTel(tmp, platform, resource, emissionLvl, plant)
			val result: DataFrame = tmpComp.join(tmpValidity, Seq("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES"), "left_outer")
			dfList = dfList :+ result
		}
		dfList.reduce(_.union(_))
	}

	import spark.implicits._

	import collection.mutable

	def calculateComp(df: DataFrame, baseCount: Double, platform: String, resource: String, emissionLvl: String, plant: String) = {
		var mapComp = new mutable.HashMap[String, Double]()
		println(platform + "_" + resource + "_" + emissionLvl + "_" + plant + " >>is calculating completeness>>>>")
		df.columns.map(colname => {
			val illegalNBR: Double = df.select(colname).where(colname + " is null or " + colname + " == '' or " + colname + " == ' '").count()
			val percentage: Double = 1.0 - illegalNBR / baseCount
			mapComp += (colname -> percentage)
		})
		mapComp.toSeq.toDF("COLNAMES", "COMPLETENESS").withColumn("PLATFORM", lit(platform)).
			withColumn("RESOURCE", lit(resource)).
			withColumn("EMISSION_LEVEL", lit(emissionLvl)).withColumn("EMISSION_LEVEL", when($"EMISSION_LEVEL" === "5", lit("NSV")).otherwise(lit(("NSVI")))).
			withColumn("ENTITY", lit(plant)).
			select("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES", "COMPLETENESS")
	}

	def calValidityBuild(df: DataFrame, platform: String, resource: String, emissionLvl: String, plant: String) = {
		var mapComp = new mutable.HashMap[String, Double]()
		println(platform + "_" + resource + "_" + emissionLvl + "_" + plant + " >>is calculating validity>>>>")
		df.columns.map(colname => {
			if (colname == "HORSEPOWER") {
				val base = df.select(colname).where(colname + " is not null and " + colname + " != '' and " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = base.select(col("HORSEPOWER").cast(DoubleType).as("HORSEPOWER")).where("HORSEPOWER > 0 and HORSEPOWER < 1000")
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
		})
		mapComp.toSeq.toDF("COLNAMES", "VALIDITY").withColumn("PLATFORM", lit(platform)).
			withColumn("RESOURCE", lit(resource)).
			withColumn("EMISSION_LEVEL", lit(emissionLvl)).withColumn("EMISSION_LEVEL", when($"EMISSION_LEVEL" === "5", lit("NSV")).otherwise(lit(("NSVI")))).
			withColumn("ENTITY", lit(plant)).
			select("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES", "VALIDITY")
	}

	def calValidityClaim(df: DataFrame, platform: String, resource: String, emissionLvl: String, plant: String) = {
		var mapComp = new mutable.HashMap[String, Double]()
		println(platform + "_" + resource + "_" + emissionLvl + "_" + plant + " >>is calculating validity>>>>")
		df.columns.map(colname => {
			if (colname == "VEHICLE_PURCHASE_DATE") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = df.filter(col("VEHICLE_PURCHASE_DATE") < col("FAILURE_DATE")).filter(col("VEHICLE_PURCHASE_DATE") > col("BUILD_DATE"))
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
			if (colname == "FAILURE_DATE") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = df.filter(col("FAILURE_DATE") > col("BUILD_DATE"))
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
			if (colname == "MILEAGE") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val addMileage = df.withColumn("Days", datediff(col("FAILURE_DATE"), col("VEHICLE_PURCHASE_DATE"))).withColumn("MAX_Mileage", when($"Days" > 0, $"Days" / 30 * 40000).otherwise(40000))
				val legalBase = addMileage.filter(col("Mileage") < col("MAX_Mileage"))
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
			if (colname == "TOTAL_COST") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = df.filter(col("TOTAL_COST") >= 0)
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
			if (colname == "LABOR_HOUR") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = df.filter(col("LABOR_HOUR") >= 0)
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
		})
		mapComp.toSeq.toDF("COLNAMES", "VALIDITY").withColumn("PLATFORM", lit(platform)).
			withColumn("RESOURCE", lit(resource)).
			withColumn("EMISSION_LEVEL", lit(emissionLvl)).withColumn("EMISSION_LEVEL", when($"EMISSION_LEVEL" === "5", lit("NSV")).otherwise(lit(("NSVI")))).
			withColumn("ENTITY", lit(plant)).
			select("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES", "VALIDITY")
	}


	def calValidityTel(df: DataFrame, platform: String, resource: String, emissionLvl: String, plant: String): Dataset[Row] = {
		var mapComp = new mutable.HashMap[String, Double]()
		println(platform + "_" + resource + "_" + emissionLvl + "_" + plant + " >>is calculating validity>>>>")
		df.columns.map(colname => {
			if (colname == "LATITUDE" || colname == "LONGITUDE" || colname == "ALTITUDE") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = base.filter(colname + " > '0'")
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
			if (colname == "OCCURRENCE_DATE_TIME") {
				val base = df.select(colname).where(colname + " is not null or " + colname + " != '' or " + colname + " != ' '")
				val baseCount: Double = base.count()
				val legalBase = df.filter($"OCCURRENCE_DATE_TIME" > $"BUILD_DATE")
				val legalNB: Double = legalBase.count()
				val percentage: Double = legalNB / baseCount
				mapComp += (colname -> percentage)
			}
		})
		mapComp.toSeq.toDF("COLNAMES", "VALIDITY").withColumn("PLATFORM", lit(platform)).
			withColumn("RESOURCE", lit(resource)).
			withColumn("EMISSION_LEVEL", lit(emissionLvl)).withColumn("EMISSION_LEVEL", when($"EMISSION_LEVEL" === "5", lit("NSV")).otherwise(lit(("NSVI")))).
			withColumn("ENTITY", lit(plant)).
			select("RESOURCE", "PLATFORM", "EMISSION_LEVEL", "ENTITY", "COLNAMES", "VALIDITY")
	}


	//load engine detial data from primary
	def loadEngineDetail: Dataset[Row] = {

		val engineDetailDF = spark.sql(
			s"""
				 |SELECT
				 |ESN_NEW as ESN,
				 |SO_NUM,
				 |BUILD_DATE,
				 |EMISSION_LEVEL,
				 |ATS_TYPE,
				 |HORSEPOWER,
				 |USERAPP,
				 |VEHICLE_TYPE,
				 |OEM_NAME,
				 |ENGINE_PLANT,
				 |IN_SERVICE_DATE_AVE_TIME_LAG,
				 |DOM_INT,
				 |WAREHOUSE,
				 |ACTUAL_IN_SERVICE_DATE,
				 |ENGINE_PLATFORM,
				 |ABO_QUALITY_PERFORMANCE_GROUP,
				 |CCI_CHANNEL_SALES_Y_N
				 | FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_DTL_TBL)}
		""".stripMargin).where("(Dom_Int!='Int'or Dom_Int is null) and Build_Date>=to_date('2014-01-01') and Build_Date<=CURRENT_DATE() and Emission_Level in ('E5','NS5','NS6','S5')")
			.withColumnRenamed("engine_serial_num", "ESN").dropDuplicates("ESN")
		engineDetailDF
	}

	//load claim data from primary
	def loadClaimDetail: Dataset[Row] = {
		val claimDetailDF = spark.sql(
			s"""
				 |SELECT ESN_NEW AS ESN,
				 |CLAIM_NUMBER,
				 |DEALER_NAME,
				 |FAILURE_PART_NUMBER,
				 |MILEAGE,
				 |FAILURE_PART,
				 |BASE_OR_ATS,
				 |COVERAGE,
				 |TOTAL_COST,
				 |LABOR_HOUR,
				 |DATA_PROVIDER,
				 |FAIL_CODE_MODE,
				 |FAILURE_DATE,
				 |CLAIM_SUBMIT_DATE,
				 |VEHICLE_PURCHASE_DATE,
				 |PAID_OPEN,
				 |PAY_DATE,
				 |Complaint as COMPLAINT,
				 |Failure_Cause as FAILURE_CAUSE,
				 |Correction as CORRECTION
				 |FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.REL_ENG_CLM_DTL_TBL)}
			 """.stripMargin)
		claimDetailDF
	}


	def loadTelDetail: Dataset[Row] = {
//		spark.table(s"${cfg(Constants.PRI_DB)}.${cfg(Constants.TEL_FC_DTL_TBL)}").createOrReplaceTempView("telematics_primary")
		val strSql: String =
			s"""
				 |SELECT TELEMATICS_PARTNER_NAME,
				 |OCCURRENCE_DATE_TIME,
				 |LATITUDE,
				 |LONGITUDE,
				 |ALTITUDE,
				 |ENGINE_SERIAL_NUMBER,
				 |SERVICE_MODEL_NAME,
				 |ACTIVE,
				 |FAULT_CODE,
				 |FAULT_CODE_DESCRIPTION,
				 |DERATE_FLAG,
				 |LAMP_COLOR,
				 |REPORT_TYPE,
				 |SHUTDOWN_FLAG,
				 |PRIORITY,
				 |FAULT_CODE_CATEGORY
				 |FROM ${cfg(Constants.PRI_DB)}.${cfg(Constants.TEL_FC_DTL_TBL)}
      """.stripMargin
		val strFilt: String =
			s"""
				 |ENGINE_SERIAL_NUMBER IN (
				 |SELECT ENGINE_DETAILS_ESN
				 |FROM ${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)})
      """.stripMargin

		val telfcDataRaw: Dataset[Row] = spark.sql(strSql)
			.filter($"Active" === "1")
			.where(strFilt)
		telfcDataRaw
	}


	def loadSignOfBuild: Dataset[Row] = {
		val buildSignSeq: Dataset[Row] = Seq(
			(java.time.LocalDate.now.toString, "ESN", 9),
			(java.time.LocalDate.now.toString, "SO_NUM", 9),
			(java.time.LocalDate.now.toString, "BUILD_DATE", 9),
			(java.time.LocalDate.now.toString, "EMISSION_LEVEL", 9),
			(java.time.LocalDate.now.toString, "USERAPP", 9),
			(java.time.LocalDate.now.toString, "ENGINE_PLATFORM", 9),
			(java.time.LocalDate.now.toString, "OEM_NAME", 6),
			(java.time.LocalDate.now.toString, "ACTUAL_IN_SERVICE_DATE", 6),
			(java.time.LocalDate.now.toString, "ENGINE_PLANT", 6),
			(java.time.LocalDate.now.toString, "VEHICLE_TYPE", 3),
			(java.time.LocalDate.now.toString, "IN_SERVICE_DATE_AVE_TIME_LAG", 1),
			(java.time.LocalDate.now.toString, "DOM_INT", 3),
			(java.time.LocalDate.now.toString, "ABO_QUALITY_PERFORMANCE_GROUP", 1),
			(java.time.LocalDate.now.toString, "CCI_CHANNEL_SALES_Y_N", 3),
			(java.time.LocalDate.now.toString, "WAREHOUSE", 1),
			(java.time.LocalDate.now.toString, "ATS_TYPE", 3),
			(java.time.LocalDate.now.toString, "HORSEPOWER", 3)
		).toDF("UPDATE_TIME", "col_Build", "SIGNIFICANCE")

		buildSignSeq
	}

	def loadSignOfClaim: Dataset[Row] = {
		val claimSigniSeq: Dataset[Row] = Seq(
			(java.time.LocalDate.now.toString, "ESN", 9),
			(java.time.LocalDate.now.toString, "CLAIM_NUMBER", 9),
			(java.time.LocalDate.now.toString, "FAILURE_PART_NUMBER", 9),
			(java.time.LocalDate.now.toString, "FAILURE_PART", 9),
			(java.time.LocalDate.now.toString, "BASE_OR_ATS", 9),
			(java.time.LocalDate.now.toString, "COVERAGE", 9),
			(java.time.LocalDate.now.toString, "TOTAL_COST", 9),
			(java.time.LocalDate.now.toString, "FAIL_CODE_MODE", 3),
			(java.time.LocalDate.now.toString, "FAILURE_DATE", 9),
			(java.time.LocalDate.now.toString, "PAY_DATE", 9),
			(java.time.LocalDate.now.toString, "LABOR_HOUR", 9),
			(java.time.LocalDate.now.toString, "MILEAGE", 9),
			(java.time.LocalDate.now.toString, "DEALER_NAME", 6),
			(java.time.LocalDate.now.toString, "DATA_PROVIDER", 6),
			(java.time.LocalDate.now.toString, "CLAIM_SUBMIT_DATE", 1),
			(java.time.LocalDate.now.toString, "VEHICLE_PURCHASE_DATE", 6),
			(java.time.LocalDate.now.toString, "PAID_OPEN", 6),
			(java.time.LocalDate.now.toString, "COMPLAINT", 6),
			(java.time.LocalDate.now.toString, "FAILURE_CAUSE", 6),
			(java.time.LocalDate.now.toString, "CORRECTION", 6)
		).toDF("UPDATE_TIME", "col_Claim", "SIGNIFICANCE")

		claimSigniSeq
	}

	def loadSignOfTel: Dataset[Row] = {
		val telSignSeq: Dataset[Row] = Seq(
			(java.time.LocalDate.now.toString, "OCCURRENCE_DATE_TIME", 9),
			(java.time.LocalDate.now.toString, "ESN", 9),
			(java.time.LocalDate.now.toString, "FAULT_CODE", 9),
			(java.time.LocalDate.now.toString, "ACTIVE", 9),
			(java.time.LocalDate.now.toString, "LATITUDE", 6),
			(java.time.LocalDate.now.toString, "LONGITUDE", 6),
			(java.time.LocalDate.now.toString, "ALTITUDE", 6),
			(java.time.LocalDate.now.toString, "FAULT_CODE_DESCRIPTION", 6),
			(java.time.LocalDate.now.toString, "REPORT_TYPE", 3),
			(java.time.LocalDate.now.toString, "FAULT_CODE_CATEGORY", 3),
			(java.time.LocalDate.now.toString, "DERATE_FLAG", 1),
			(java.time.LocalDate.now.toString, "SHUTDOWN_FLAG", 1)
		).toDF("UPDATE_TIME", "col_Telematics", "SIGNIFICANCE")

		telSignSeq
	}


}
