package mck.qb.columbia.modelInput.uv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO._
import mck.qb.library.{Job, Util}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.mutable.WrappedArray
import scala.util.Try

abstract class DetectMasterJob extends Job with Transport {

  import spark.implicits._
  import spark.sql

  private val numPartitions = 256
  override val logger: Logger = Logger.getLogger(this.getClass)

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_UV_MST)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString

  def getTargetDB: String
  def getEngType: String

  protected def partitionByESN = partitionBy(numPartitions, $"REL_ESN") _

  override def run(): Unit = {

    logger.log(Level.INFO, s"Starting Job.")

    withShufflePartition(numPartitions) {

      // Step 1: Get all incidents e.g. ALL_INCIDENTS_NEW_B
      val incidentsAllColumns = readIncidents(cfg)

      // Step 2: Build Online Incident Tracker e.g. ALL_INCIDENTS_NEW_B
      val onlineIncidentTracker_0 = buildOnlineIncidentTracker(incidentsAllColumns, cfg)

      // Step 3: Check if before and after counts are correct between original incidents and left joined.
      assert(incidentsAllColumns.count() == onlineIncidentTracker_0.count(), "Incidents count did not match")
      val onlineIncidentTracker: Dataset[Row] = onlineIncidentTracker_0.where("REL_FAILURE_DATE>REL_BUILD_DATE OR REL_FAILURE_DATE IS NULL")
      // Step 4: Build PERSISTED_INCIDENT_XX DF
      var doingActualBuild = true // DO NOT MODIFY

      if (cfg(Constants.FIRST_TIME_RUN).equalsIgnoreCase("true")) {
        logger.log(Level.INFO, "Automation for persisted_incident_0 table")
        doingActualBuild = false
      }

      val writableOnlineIncidentTracker = if (doingActualBuild) {
        buildAndPersistIncidentsHistory(onlineIncidentTracker, cfg)
      } else {
        Util.dfZipWithIndex(onlineIncidentTracker)
      }
      showCoverage(onlineIncidentTracker, writableOnlineIncidentTracker)

      // Step 4: Save data to e.g. quality_b_mi.FULL_FEATURES_PERSISTED.
      logger.warn(s"Saved data to ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.FULL_FET_TBL)}_${getEngType}")

      Util.saveData(writableOnlineIncidentTracker, cfg(Constants.MODEL_IP_DB), s"${cfg(Constants.FULL_FET_TBL)}_${getEngType}", cfg(Constants.HDFS_PATH))

      // Step 5: TODO: used to save a parquet file, why do we need this?
      val foundInColumns = onlineIncidentTracker.columns.filter(_.startsWith("FOUND_IN"))
      Util.saveData(writableOnlineIncidentTracker.drop(foundInColumns: _*), cfg(Constants.FET_DB), s"${cfg(Constants.ENV)}_${getEngType}_WITHOUT_FOUND_IN_COLS",cfg(Constants.HDFS_PATH))
      logger.warn(s"Saved parquet file to ${cfg(Constants.ENV)}_${getEngType}_WITHOUT_FOUND_IN_COLS")
    }

    //Create persissted_incidents_0 table for the first run
    if (cfg(Constants.FIRST_TIME_RUN).equalsIgnoreCase("true")) {
      val persistedincidents = sql(s"create table ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.REF_PER_INCS_TBL)}_${getEngType}_0 like ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.FULL_FET_TBL)}_${getEngType}")
      logger.log(Level.INFO, s"${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.REF_PER_INCS_TBL)}_${getEngType}_0 table got created successfully")
    }
  }
  //todo: what's this? what do we want to do?
  def joinSpecificFeatures(commonJoinDF: DataFrame): DataFrame =
    commonJoinDF

  def buildOnlineIncidentTracker(incidentsAllColumns: DataFrame, cfg: String => String): DataFrame = {

    //todo:This Dataset is never used, is it to be deleted?
    val incidents: Dataset[Row] = incidentsAllColumns.select("REL_ESN", "TEL_FAILURE_DATE", "REL_FAILURE_DATE").distinct().cache()

    val engineBuildDf = readEngineBuild(cfg)

    // left joining all data sources to the Incident table. The number of incidents before and after join should remain
    // the same, wich means all the sources doesnt have duplicates and are at the same grain level.
    //todo:this Dataset is never used
    val incidentsWithTelFaultCodeList: Dataset[Row] = incidentsAllColumns.select("REL_ESN", "TEL_FAILURE_DATE", "REL_FAILURE_DATE", "Fault_Code_LIST").distinct().cache()

    val commonJoinDF =
      incidentsAllColumns.join(engineBuildDf, "REL_ESN").drop(engineBuildDf("REL_ESN"))

    val extendedDF = joinSpecificFeatures(commonJoinDF)

    extendedDF.
      withColumn("CMP_FAULT_CODE",
        when(
          $"Fault_Code_LIST".isNotNull && size($"Fault_Code_LIST") > 0, $"Fault_Code_LIST"
        ).when(
          $"nonIm_Fault_Code_LIST".isNotNull && size($"nonIm_Fault_Code_LIST") > 0, array($"nonIm_Fault_Code_LIST" (0))
        ).otherwise(null))
      .withColumn("Mileage_LIST",$"Mileage_LIST".cast("array<bigint>"))
      .withColumn("nonIm_Mileage_LIST",$"nonIm_Mileage_LIST".cast("array<bigint>"))
      .withColumn("REL_CMP_ENGINE_MILES_LIST",Util.pickUp($"REL_CMP_ENGINE_MILES_LIST",Util.pickUp($"Mileage_LIST",$"nonIm_Mileage_LIST")))
      .withColumn("WORK_HOUR_LIST",$"WORK_HOUR_LIST".cast("array<bigint>"))
      .withColumn("nonIm_WORK_HOUR_LIST",$"nonIm_WORK_HOUR_LIST".cast("array<bigint>"))
      .withColumn("REL_CMP_ENGINE_WORK_HOUR_LIST",$"REL_CMP_ENGINE_WORK_HOUR_LIST".cast("array<bigint>"))
      .withColumn("REL_CMP_ENGINE_WORK_HOUR_LIST",Util.pickUp($"REL_CMP_ENGINE_WORK_HOUR_LIST",Util.pickUp($"WORK_HOUR_LIST",$"nonIm_WORK_HOUR_LIST")))
      .withColumn("CALIBRATION_VERSION_LIST",$"CALIBRATION_VERSION_LIST".cast("array<string>"))
      .withColumn("nonIm_CALIBRATION_VERSION_LIST",$"nonIm_CALIBRATION_VERSION_LIST".cast("array<string>"))
      .withColumn("CALIBRATION_VERSION_LIST",Util.pickUpString($"CALIBRATION_VERSION_LIST",$"nonIm_CALIBRATION_VERSION_LIST"))
      .drop("Mileage_LIST","nonIm_Mileage_LIST","nonIm_CALIBRATION_VERSION_LIST","WORK_HOUR_LIST","nonIm_WORK_HOUR_LIST")
      .withColumn("CMP_MILEAGE_LIST", when(
          $"REL_CMP_ENGINE_MILES_LIST".isNotNull && size($"REL_CMP_ENGINE_MILES_LIST") > 0 ,maxArrayUDF($"REL_CMP_ENGINE_MILES_LIST")
      ).otherwise(null))
      .withColumn("REL_CMP_ENGINE_WORK_HOUR_LIST", when(
        $"REL_CMP_ENGINE_WORK_HOUR_LIST".isNotNull && size($"REL_CMP_ENGINE_WORK_HOUR_LIST") > 0, maxArrayUDF($"REL_CMP_ENGINE_WORK_HOUR_LIST")
      ).otherwise(null))
      //.withColumn("CMP_MILEAGE_LIST",coalesce($"CMP_MILEAGE_LIST",maxArrayUDF($"Mileage_LIST"),maxArrayUDF($"nonIm_Mileage_LIST")))
      .cache()
  }

  def buildAndPersistIncidentsHistory(onlineIncidentTracker: DataFrame, cfg: String => String): DataFrame = {

    // This will get the most recently persisted build number
    val previousPersistedIncidentsTables: Array[Int] = sql(s"show tables in ${cfg(Constants.MODEL_IP_DB)}").select("tableName").collect().
      map(_.getString(0).toUpperCase).filter(_.startsWith(cfg(Constants.REF_PER_INCS_TBL)+"_"+getEngType+"_")).
      map(_.replace(cfg(Constants.REF_PER_INCS_TBL)+"_"+getEngType+"_", "").toInt)

    val previousBuildNum = if (previousPersistedIncidentsTables.nonEmpty) previousPersistedIncidentsTables.max else 0

    val columnsToConsiderADelta = columnTypes.keys.toList
    val columnMap = columnsToConsiderADelta.map(column => (column, column + "_HISTORIC")).toMap

    val allColumns = List("ID", "GROUP_BY_DATE", "GROUP_BY_ID") ::: columnsToConsiderADelta
    assert(allColumns.toSet.size == allColumns.size)

    // QUALITY_UPDATE.PERSISTED_INCIDENTS
    val historic = sql(s"select * from ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.REF_PER_INCS_TBL)}_${getEngType}_${previousBuildNum}").
      select(allColumns.head, allColumns.tail: _*).withColumn("IN_PREVIOUS", lit(1))

    //QI-742 until this is done, it will error here when we add a new column that wasn't in the previous run
    var current = onlineIncidentTracker.withColumn("ID", lit(null) cast "long").
      select(allColumns.head, allColumns.tail: _*).drop("ID").
      withColumn("IN_CURRENT", lit(1))

    columnsToConsiderADelta.foreach(column => {
      current = current.withColumnRenamed(column, columnMap(column))
    })

    val dfToCheckDelta = historic.join(current, Seq("GROUP_BY_ID", "GROUP_BY_DATE"), "outer").
      withColumn("IN_INTERSECTION",
        when($"IN_CURRENT" === 1 && $"IN_PREVIOUS" === 1, lit(1)).
          otherwise(null)
      ).cache()
    // dfToCheckDelta.count()

    // For lost rows, I'm going to need to find the claim they went to
    val lostRows = dfToCheckDelta.filter($"IN_PREVIOUS".isNotNull && $"IN_INTERSECTION".isNull)
    // For new rows, I'm going to have to give them a new id
    val newRows = dfToCheckDelta.filter($"IN_CURRENT".isNotNull && $"IN_INTERSECTION".isNull)
    // For persisted rows, will give a new ID if needed
    val intersectionRows = dfToCheckDelta.filter($"IN_INTERSECTION".isNotNull)

    assert(lostRows.count() + newRows.count() + intersectionRows.count() == dfToCheckDelta.count())

    val intListToSet = udf((list: scala.collection.mutable.WrappedArray[Int]) => if (list == null) null else list.toSet.toList.sorted)
    val doubleListToSet = udf((list: scala.collection.mutable.WrappedArray[Double]) => if (list == null) null else list.toSet.toList.sorted)
    val stringListToSet = udf((list: scala.collection.mutable.WrappedArray[String]) => if (list == null) null else list.toSet.toList.sorted)
    val longListToSet = udf((list: scala.collection.mutable.WrappedArray[Long]) => if (list == null) null else list.toSet.toList.sorted) // == JR

    spark.sparkContext.setCheckpointDir(s"${cfg(Constants.HDFS_PATH)}/data/tmp/checkpoint")

    var deltaRows = intersectionRows.withColumn("DELTAS", lit(0))
    // deltaRows.count()

    columnMap.zipWithIndex.foreach {
      case ((oldColumn, newColumn), index) => {
        val columnType = columnTypes(oldColumn)
        if (columnType != "NOT_OF_LIST_TYPE") {
          // We have a column of type list, so lets convert it to a set
          val oldColColumn = col(oldColumn)
          val newColColumn = col(newColumn)
          val comparison = columnType match {
            case "string" => stringListToSet(oldColColumn) =!= stringListToSet(newColColumn)
            case "integer" => intListToSet(oldColColumn) =!= intListToSet(newColColumn)
            case "double" => doubleListToSet(oldColColumn) =!= doubleListToSet(newColColumn)
            case "long" => longListToSet(oldColColumn) =!= longListToSet(newColColumn) // == JR
            case "date" => oldColColumn =!= newColColumn // == JR
          }
          deltaRows = deltaRows.withColumn("DELTAS",
            when(comparison,
              $"DELTAS" + 1).
              otherwise($"DELTAS")).cache()
        } else {
          deltaRows = deltaRows.withColumn("DELTAS",
            when(col(oldColumn) =!= col(newColumn),
              $"DELTAS" + 1).
              otherwise($"DELTAS")).cache()
        }
      }
        logger.info(s"Persisting table ${oldColumn}.")
      // deltaRows.count()
    }
    // deltaRows.count()

    //Below we are trying to find out what has changed since last data refresh.
    val changedRows = deltaRows.select("GROUP_BY_ID", "GROUP_BY_DATE", "DELTAS").filter($"DELTAS" > 0).
      withColumn("DELTA_HAPPENED", lit(true)).drop("DELTAS").cache()
    // changedRows.count()

    val intersectionRowsWithDelta = intersectionRows.
      join(changedRows, Seq("GROUP_BY_ID", "GROUP_BY_DATE"), "left_outer").
      withColumn("DELTA_HAPPENED", coalesce($"DELTA_HAPPENED", lit(false)))

    val recordsNeedingNewId = intersectionRowsWithDelta.filter($"DELTA_HAPPENED").
      select("GROUP_BY_ID", "GROUP_BY_DATE").union(newRows.select("GROUP_BY_ID", "GROUP_BY_DATE"))

    val maxIdRow = historic.agg(max("ID")).collect()(0)
    val maxId = if (maxIdRow.isNullAt(0)) 0 else maxIdRow.getLong(0).toInt

    val newIds = Util.dfZipWithIndex(df = recordsNeedingNewId, offset = maxId + 1, colName = "NEW_ID").
      withColumn("ID", lit(null) cast "bigint")
    val oldIds = intersectionRowsWithDelta.filter(!$"DELTA_HAPPENED").
      withColumn("NEW_ID", lit(null) cast "bigint").select("NEW_ID", "GROUP_BY_ID", "GROUP_BY_DATE", "ID")

    val reconciledIds = newIds.union(oldIds).withColumn("ID", coalesce($"NEW_ID", $"ID")).drop("NEW_ID")

//    logger.info("Number of records merged into claims: " + lostRows.count())
//    logger.info("Number of new incidents: " + newRows.count())
//    logger.info("Number of changed incidents: " + intersectionRowsWithDelta.filter($"DELTA_HAPPENED").count())
//    logger.info("Number of unchanged incidents: " + intersectionRowsWithDelta.filter(!$"DELTA_HAPPENED").count())

    val telonly: Column = ($"REL_FAILURE_DATE".isNull.or($"REL_FAILURE_DATE" === "")).and($"TEL_FAILURE_DATE".isNotNull.and($"TEL_FAILURE_DATE"=!=""))
    val relonly: Column = ($"REL_FAILURE_DATE".isNotNull.and($"REL_FAILURE_DATE" =!= "")).and($"TEL_FAILURE_DATE".isNull.or($"TEL_FAILURE_DATE"===""))
    val both: Column = ($"REL_FAILURE_DATE".isNotNull.and($"REL_FAILURE_DATE" =!= "")).and($"TEL_FAILURE_DATE".isNotNull.and($"TEL_FAILURE_DATE"=!=""))
    val telAll: Column = ($"REL_CMP_COVERAGE".isNull.or($"REL_CMP_COVERAGE" === "")).and($"REL_FAILURE_DATE".isNull.or($"REL_FAILURE_DATE" === "")).and($"TEL_FAILURE_DATE".isNotNull.and($"TEL_FAILURE_DATE"=!=""))

    val output_0 = reconciledIds.join(onlineIncidentTracker, Seq("GROUP_BY_DATE", "GROUP_BY_ID"))
      .withColumn("ANALYTICS_RUN_DATE", current_timestamp())

    val ref_ft: DataFrame = spark.table(s"${cfg(Constants.REF_DB)}.${cfg(Constants.REF_TEL_FT_ESN_TBL)}").select("ESN").withColumn("flag", lit(1))
    val allInOne: DataFrame = output_0.join(ref_ft,output_0("REL_ESN")===ref_ft("ESN"),"left")
    val fieldTest: DataFrame = allInOne.where($"flag"===1).withColumn("SOURCE",lit("TEL-FT"))
    val noFieldTest: DataFrame = allInOne.where($"flag" isNull)
      .withColumn("SOURCE", when(telonly, "TEL-No FT")
        .when(relonly,"REL_ONLY")
          .when(both,"BOTH")
      )
    val output: DataFrame = fieldTest.union(noFieldTest).drop("flag")
      .withColumn("REL_CMP_COVERAGE",when(telAll,"TEL_ALL").otherwise($"REL_CMP_COVERAGE"))

    logger.info(s"Writing historic dataset ${cfg(Constants.MODEL_IP_DB)}.${cfg(Constants.REF_PER_INCS_TBL)}_${getEngType}_${previousBuildNum + 1}.")
    Util.saveData(output, cfg(Constants.MODEL_IP_DB), s"${cfg(Constants.REF_PER_INCS_TBL)}_${getEngType}_${previousBuildNum + 1}", cfg(Constants.HDFS_PATH))

    output.drop("ANALYTICS_RUN_DATE")
  }

  def showCoverage(onlineIncidentTracker: DataFrame, writableOnlineIncidentTracker: DataFrame): Unit = {
    val foundInColumns = onlineIncidentTracker.columns.filter(_.startsWith("FOUND_IN"))
    val groupByStatements = foundInColumns.map(column => (column, "sum")).toMap
    val coverageCounts = writableOnlineIncidentTracker.select("ID", foundInColumns: _*).agg(
      groupByStatements
    )
    coverageCounts.show(false)

  }

  def readIncidents(cfg: String => String): DataFrame = {

    // QUALITY_COMPUTE.ALL_INCIDENTS_NEW
    partitionByESN(sql(
      s"""SELECT * FROM ${cfg(Constants.FET_DB)}.${cfg(Constants.ALL_INC_TBL)}_$getEngType""".stripMargin).
      withColumn("FOUND_IN_INCIDENT_TRACKER", lit(1))
    ).cache()
  }

  def readEngineBuild(cfg: String => String): DataFrame = {
    //QUALITY_COMPUTE.REL_ENGINE_FEATURES
    load(numPartitions, $"ENGINE_DETAILS_ESN")(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}",
      "ENGINE_DETAILS_ESN",
      "REL_SO_NUM",
      "REL_BUILD_DATE",
      "REL_EMISSION_LEVEL",
      "REL_ATS_TYPE",
      "REL_HORSEPOWER",
      "REL_USER_APPL_DESC",
      "REL_VEHICLE_TYPE",
      "REL_OEM_NAME",
      "REL_ENGINE_PLANT",
      "REL_IN_SERVICE_DATE_AVE_TIME_LAG",
      "REL_ENGINE_TYPE_DESC",
      "REL_WAREHOUSE",
      "REL_ACTUAL_IN_SERVICE_DATE",
      "REL_ENGINE_PLATFORM",
      "REL_ENGINE_GROUP_DESC",
      "REL_ABO_QUALITY_PERFORMANCE_GROUP",
      "REL_CCI_CHANNEL_SALES_Y_N",
      "REL_BUILD_YEAR",
      "REL_BUILD_MONTH",
      "REL_BUILD_DAY_OF_MONTH",
      "REL_BUILD_DAY_OF_WEEK",
      "REL_BUILT_ON_WEEKEND",
      "REL_Normal_EMISSION_LEVEL",
      "REL_ENGINE_NAME_DESC",
      "REL_CMP_TIME_DIFF_BUILDDATE_TO_INSERVICEDATE_DAYS",
      "REL_CMP_ENGINE_NAME",
      "REL_OEM_NORMALIZED_GROUP").
      withColumnRenamed("ENGINE_DETAILS_ESN", "REL_ESN").
      dropDuplicates("REL_ESN").
      withColumn("FOUND_IN_RELIABILITY_ENGINE", lit(1))
  }

  val columnTypes = Map(
    ("REL_CMP_FAIL_CODE_LIST", "string"),
    //          ("Fault_Code_LIST","integer"),
    ("Fault_Code_LIST", "string"), // == JR

    //          ("nonIm_Fault_Code_LIST","integer"),
    ("nonIm_Fault_Code_LIST", "string"), // == JR

    //("INS_FI_FAULT_CODE_LST","integer"),
    ("REL_OEM_NAME", "NOT_OF_LIST_TYPE"),
    //("GREEN_WRENCH_LIST","string"),
    //("REL_ANALYSIS_RATE_CAT","NOT_OF_LIST_TYPE"),
    //          ("REL_CMP_ENGINE_MILES_LIST","double"),
    ("REL_CMP_ENGINE_MILES_LIST", "long"), // == JR

    ("REL_CMP_SUM_NET_AMOUNT", "NOT_OF_LIST_TYPE"),
    //("INCDT_ISSUE_LABEL","NOT_OF_LIST_TYPE"),
    ("REL_CMP_COVERAGE", "NOT_OF_LIST_TYPE"),
    //          ("REL_FAILURE_DATE","string"),
    ("REL_FAILURE_DATE", "date"), // == JR

    ("REL_CMP_CLAIM_DATE_LIST", "string")
  )

  def ensureUpdateMode(cfg: String => String) = {
    spark.catalog.setCurrentDatabase(s"${cfg(Constants.FET_DB)}")

    if (!spark.catalog.tableExists(s"${cfg(Constants.REF_PER_INCS_TBL)}_${getEngType}")) {

      //QUALITY_COMPUTE.FULL_FEATURES_PERSISTED
      val incidentTracker = sql(s"select * from ${cfg(Constants.FET_DB)},${cfg(Constants.FULL_FET_TBL)}_${getEngType}").
        withColumn("ANALYTICS_RUN_DATE", current_timestamp())

      // QUALITY_UPDATE.PERSISTED_INCIDENTS
      Util.saveData(incidentTracker, cfg(Constants.FET_DB), cfg(Constants.REF_PER_INCS_TBL)+"_"+getEngType,cfg(Constants.HDFS_PATH))
    }
  }

  val maxArrayUDF = {
    udf((arr: WrappedArray[Long]) => {
      Try {
        def f(max: Long, e: Long): Long = {
          if (max < e) {
            e
          } else {
            max
          }
        }

        val m = arr.foldLeft(arr(0))(f)
        Some(m)
      }.getOrElse(None)
    })
  }

    def getCurrentTimeStampString: String = {
      val today: java.util.Date = Calendar.getInstance.getTime
      val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val now: String = timeFormat.format(today)
      val re = java.sql.Timestamp.valueOf(now)
      re.toString.replace(" ", "--").replace(":", "-").replace(".0", "")
    }
  }
