package mck.qb.columbia.monitoring

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.Job
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object ESNMasterProcess extends  Job{
//	val myConf : Config = AppConfig.getMyConfig(Constants.SSAP_ESN_MASTER)
//	logger.warn("Config utilized - (%s)".format(myConf))
//	val cfg : String=> String = myConf.getString

	import spark.implicits._
	/**
		* Intended to be implemented for every job created.
		*/
	override def run(): Unit = {
		val engineDF = loadEngineDetail("rel_engine_detail").withColumn("test_plant", getNormPlatform($"engine_platform")).
			withColumn("test_entity", getNormEntity($"ENGINE_PLANT")).withColumn("test_emission", getNormEmission($"EMISSION_LEVEL"))



	}
	def loadEngineDetail(df : String) = {

		val engineDetailDF = spark.sql(
			s"""
				 |SELECT
				 |ESN_NEW as ESN,
				 |ENGINE_PLATFORM,
				 |EMISSION_LEVEL,
				 |ENGINE_PLANT,
				 |DISPLACEMENT,
				 |BUILD_DATE,
				 |DOM_INT
				 | FROM primary.$df
		""".stripMargin).where("(Dom_Int!='Int'or Dom_Int is null) and Build_Date>=to_date('2014-01-01') and Build_Date<=CURRENT_DATE() and Emission_Level in ('E5','NS5','NS6','S5')")
			.withColumnRenamed("engine_serial_num", "ESN").dropDuplicates("ESN")
		engineDetailDF
	}


	def getNormPlatform: UserDefinedFunction = udf {
		(ENGINE_PLATFORM: String) => {
			if (ENGINE_PLATFORM != null) {
				ENGINE_PLATFORM.toUpperCase match {
					case "ISD4.0" => "D4.0"
					case "ISD4.5" => "D4.5"
					case "ISD6.7" => "D6.7"
					case "QSB6.7Tier4Final" => "QSB6.7"
					case "QSLTier4Final" => "QSL"
					case "QSM10.8" => "QSM11"
					case "B6.7e5" => "B6.7"
					case "ISB4.0" => "B4.0"
					case "ISB4.5" => "B4.5"
					case "ISB5.9" => "B5.9"
					case "ISB6.2" => "B6.2"
					case "ISB6.7" => "B6.7"
					case "ISC8.3" => "C8.3"
					case "ISF3.8" => "F3.8"
					case "ISF2.8" => "F2.8"
					case "ISF4.5" => "F4.5"
					case "L9" | "ISL8.9" => "L8.9"
					case "ISL9.5" => "L9.5"
					case "ISM10.8" | "ISM10.9" => "M11"
					case "X10.5" | "X11.0" => "X11"
					case "X11.8" | "X12.0" => "X12"
					case "X13.0" => "X13"
					case  "ISZ14.0" => "Z14"
					case "ISZ13.0" | "ISZ13" => "Z13"
					case "IS43.0" | "ISF." | "" => "Unknown"
					case _ => ENGINE_PLATFORM.toString
				}
			} else {
				"Unknown"
			}
		}
	}


	def getNormEntity = udf {
		(ENGINE_PLANT : String) => {
			if(ENGINE_PLANT != null){
				ENGINE_PLANT.toUpperCase match {
					case "" => "Unknown"
					case _ => ENGINE_PLANT.toString
				}
			} else {
				"Unknown"
			}
		}
	}


	def getNormEmission = udf {
		(EMISSION_LEVEL : String) => {
			if(EMISSION_LEVEL != null){
				EMISSION_LEVEL.toUpperCase match {
					case "T3" | "Tier3" => "CS3"
					case "T4" => "CS4"
					case "E3" | "NS3" | "国三" => "NSIII"
					case "E4" | "NS4" | "国四" => "NSIV"
					case "JingV" | "JingV++" | "京V" | "E5" | "NS5" | "S5" => "NSV"
					case "E6" | "NS6" | "国六"=> "NSVI"
					case "" | "EPA2007" | "无" => "Unknown"
					case _ => EMISSION_LEVEL.toString
				}
			} else  {
				"Unknown"
			}
		}
	}



}
