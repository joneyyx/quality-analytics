package mck.qb.columbia.modelInput.detect.om

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.IO.Transport
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

abstract class OMBuildVolumeModelInputJob extends Job with Transport {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.MIP_DETECT_OM_BV)
  logger.warn("Config utilized - (%s)".format(myConf))
  override val cfg: String => String = myConf.getString


  def getEngType: String

  def getTargetDB: String

  override def run(): Unit = {

    logger.warn("Info: Reading: " + cfg(Constants.FET_DB) + "." + cfg(Constants.REL_ENG_FET_TBL))
    val populationBaseDF0 =
      load(s"${cfg(Constants.FET_DB)}.${cfg(Constants.REL_ENG_FET_TBL)}", //REL_ENGINE_FEATURES
        "ENGINE_DETAILS_ESN",
        "REL_BUILD_DATE",
        "REL_ENGINE_NAME_DESC",
        "REL_USER_APPL_DESC",
        "REL_CMP_ENGINE_NAME",
        "REL_OEM_NORMALIZED_GROUP",
        "FIRST_CONNECTION_TIME",
        "REL_ENGINE_PLANT"
      )
        .withColumnRenamed("FIRST_CONNECTION_TIME","FIRST_CONNECTION_DATE")
        .withColumn("BUILD_YEAR", substring($"REL_BUILD_DATE", 1, 4))
        .withColumn("REL_QUARTER_BUILD_DATE", concat(year($"REL_BUILD_DATE"), lit("-Q"), quarter($"REL_BUILD_DATE")))
        .withColumn("REL_ENGINE_NAME_DESC",concat_ws("_",$"REL_ENGINE_NAME_DESC",$"REL_ENGINE_PLANT"))
        .drop("REL_ENGINE_PLANT")
        .filter($"BUILD_YEAR" > 2014).persist(StorageLevel.MEMORY_AND_DISK)

    val populationBaseDF = Util.filterEnginesByType(cfg, populationBaseDF0, getEngType, esnColName = "ENGINE_DETAILS_ESN").coalesce(20).persist(StorageLevel.MEMORY_AND_DISK)

    // NOT All
    val popDF0 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE").distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_USER_APPL_DESC
    val popDF11 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE").
      withColumn("REL_USER_APPL_DESC", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_OEM_NORMALIZED_GROUP
    val popDF12 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR", "REL_USER_APPL_DESC", "REL_QUARTER_BUILD_DATE").
      withColumn("REL_OEM_NORMALIZED_GROUP", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_QUARTER_BUILD_DATE
    val popDF13 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR", "REL_USER_APPL_DESC", "REL_OEM_NORMALIZED_GROUP").
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP
    val popDF21 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE").
      withColumn("REL_USER_APPL_DESC", lit("All")).
      withColumn("REL_OEM_NORMALIZED_GROUP", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_USER_APPL_DESC, REL_QUARTER_BUILD_DATE
    val popDF22 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP").
      withColumn("REL_USER_APPL_DESC", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_OEM_NORMALIZED_GROUP, REL_QUARTER_BUILD_DATE
    val popDF23 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR", "REL_USER_APPL_DESC").
      withColumn("REL_OEM_NORMALIZED_GROUP", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val popDF24 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      withColumn("BUILD_YEAR", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP, REL_QUARTER_BUILD_DATE
    val popDF31 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "BUILD_YEAR").
      withColumn("REL_USER_APPL_DESC", lit("All")).
      withColumn("REL_OEM_NORMALIZED_GROUP", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_USER_APPL_DESC, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val popDF32 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_OEM_NORMALIZED_GROUP").
      withColumn("REL_USER_APPL_DESC", lit("All")).
      withColumn("BUILD_YEAR", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_OEM_NORMALIZED_GROUP, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val popDF33 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC").
      withColumn("REL_OEM_NORMALIZED_GROUP", lit("All")).
      withColumn("BUILD_YEAR", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val popDF4 = populationBaseDF.
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC").
      withColumn("REL_USER_APPL_DESC", lit("All")).
      withColumn("REL_OEM_NORMALIZED_GROUP", lit("All")).
      withColumn("BUILD_YEAR", lit("All")).
      withColumn("REL_QUARTER_BUILD_DATE", lit("All")).distinct().
      select("REL_CMP_ENGINE_NAME", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "BUILD_YEAR", "REL_OEM_NORMALIZED_GROUP", "REL_QUARTER_BUILD_DATE")


    val unionDF = popDF0.union(popDF11).union(popDF12).union(popDF13).union(popDF21).union(popDF22).union(popDF23).union(popDF24).union(popDF31).union(popDF32).union(popDF33).union(popDF4).coalesce(400).distinct()

    logger.warn(s"Reading: ${cfg(Constants.FET_DB)}.${cfg(Constants.OCM_REL_MTR_ALL_TBL)}")
    val fulltimeDF = load(s"${cfg(Constants.FET_DB)}.${cfg(Constants.OCM_REL_MTR_ALL_TBL)}", "CALC_DATE").distinct()

    // coalesce the fulltimeDF to one partition (sparsely distributed to 400 partitions by default)
    // careful with cross join - default partitions of 400 ^ 2 = 160000 partitions
    val crossDF0 = unionDF.coalesce(20).crossJoin(fulltimeDF.coalesce(1))

    val crossDF: DataFrame = spark.createDataFrame(crossDF0.rdd,crossDF0.schema)

    val popAliasDF = populationBaseDF.
      select(
        $"ENGINE_DETAILS_ESN".alias("ESN"),
        $"REL_BUILD_DATE".alias("BUILD_DATE"),
        $"REL_ENGINE_NAME_DESC".alias("ENGINE_NAME"),
        $"REL_USER_APPL_DESC".alias("USER_APPL"),
        $"REL_CMP_ENGINE_NAME".alias("ENGINE_FAMILY"),
        $"BUILD_YEAR".alias("B_YEAR"),
        $"REL_OEM_NORMALIZED_GROUP".alias("OEM"),
        $"REL_QUARTER_BUILD_DATE".alias("B_QUARTER"),
        $"FIRST_CONNECTION_DATE".alias("FIRST_CONNECTION_DATE")
      ).coalesce(20).persist(StorageLevel.MEMORY_AND_DISK)

    val joinCondition =
      crossDF("CALC_DATE") >= popAliasDF("BUILD_DATE") &&
        crossDF("REL_ENGINE_NAME_DESC") === popAliasDF("ENGINE_NAME") &&
        crossDF("REL_USER_APPL_DESC") === popAliasDF("USER_APPL") &&
        crossDF("REL_CMP_ENGINE_NAME") === popAliasDF("ENGINE_FAMILY") &&
        crossDF("BUILD_YEAR") === popAliasDF("B_YEAR") &&
        crossDF("REL_QUARTER_BUILD_DATE") === popAliasDF("B_QUARTER") &&
        crossDF("REL_OEM_NORMALIZED_GROUP") === popAliasDF("OEM")

    val granDF = crossDF.join(popAliasDF, joinCondition, joinType = "left_outer")

    // NOT All
    val aggFullDF = granDF.
      groupBy(
        "CALC_DATE",
        "REL_CMP_ENGINE_NAME",
        "REL_ENGINE_NAME_DESC",
        "REL_USER_APPL_DESC",
        "BUILD_YEAR",
        "REL_QUARTER_BUILD_DATE",
        "REL_OEM_NORMALIZED_GROUP"
      ).agg(
      count("ESN").alias("ESN_0"),
      count("FIRST_CONNECTION_DATE") as "CON_0"
    ).cache()

    // REL_USER_APPL_DESC
    val count11 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER").
      agg(
        count("ESN").alias("count_esn_11"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_11"
      )

    // REL_OEM_NORMALIZED_GROUP
    val count12 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "B_QUARTER").
      agg(
        count("ESN").alias("count_esn_12"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_12"
      )

    // REL_QUARTER_BUILD_DATE
    val count13 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM").
      agg(
        count("ESN").alias("count_esn_13"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_13"
      )

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP
    val count21 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "B_YEAR", "B_QUARTER").
      agg(
        count("ESN").alias("count_esn_21"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_21"
      )

    // REL_USER_APPL_DESC, REL_QUARTER_BUILD_DATE
    val count22 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "B_YEAR", "OEM").
      agg(
        count("ESN").alias("count_esn_22"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_22"
      )

    // REL_OEM_NORMALIZED_GROUP, REL_QUARTER_BUILD_DATE
    val count23 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "B_YEAR", "USER_APPL").
      agg(
        count("ESN").alias("count_esn_23"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_23"
      )

    // BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val count24 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "OEM", "USER_APPL").
      agg(
        count("ESN").alias("count_esn_24"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_24"
      )

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP, REL_QUARTER_BUILD_DATE
    val count31 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "B_YEAR").
      agg(
        count("ESN").alias("count_esn_31"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_31"
      )

    // REL_USER_APPL_DESC, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val count32 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "OEM").
      agg(
        count("ESN").alias("count_esn_32"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_32"
      )

    // REL_OEM_NORMALIZED_GROUP, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val count33 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY", "USER_APPL").
      agg(
        count("ESN").alias("count_esn_33"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_33"
      )

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val count4 = popAliasDF.
      select("ESN", "BUILD_DATE", "ENGINE_NAME", "USER_APPL", "ENGINE_FAMILY", "B_YEAR", "OEM", "B_QUARTER","FIRST_CONNECTION_DATE").
      groupBy("BUILD_DATE", "ENGINE_NAME", "ENGINE_FAMILY").
      agg(
        count("ESN").alias("count_esn_4"),
        count("FIRST_CONNECTION_DATE") as "CONNECTION_4"
      )

    // REL_USER_APPL_DESC
    val df11 = aggFullDF.join(count11,
      aggFullDF("CALC_DATE") >= count11("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count11("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count11("ENGINE_FAMILY") &&
        aggFullDF("BUILD_YEAR") === count11("B_YEAR") &&
        aggFullDF("REL_QUARTER_BUILD_DATE") === count11("B_QUARTER") &&
        aggFullDF("REL_OEM_NORMALIZED_GROUP") === count11("OEM"), joinType = "left_outer").
      filter($"REL_USER_APPL_DESC" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_11").alias("ESN_11"),
        sum("CONNECTION_11").alias("CON_11")
      )

    // REL_OEM_NORMALIZED_GROUP
    val df12 = aggFullDF.join(count12,
      aggFullDF("CALC_DATE") >= count12("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count12("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count12("ENGINE_FAMILY") &&
        aggFullDF("BUILD_YEAR") === count12("B_YEAR") &&
        aggFullDF("REL_USER_APPL_DESC") === count12("USER_APPL") &&
        aggFullDF("REL_QUARTER_BUILD_DATE") === count12("B_QUARTER"), joinType = "left_outer").
      filter($"REL_OEM_NORMALIZED_GROUP" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_12").alias("ESN_12"),
        sum("CONNECTION_12").alias("CON_12")
      )

    // REL_QUARTER_BUILD_DATE
    val df13 = aggFullDF.join(count13,
      aggFullDF("CALC_DATE") >= count13("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count13("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count13("ENGINE_FAMILY") &&
        aggFullDF("BUILD_YEAR") === count13("B_YEAR") &&
        aggFullDF("REL_OEM_NORMALIZED_GROUP") === count13("OEM") &&
        aggFullDF("REL_USER_APPL_DESC") === count13("USER_APPL"), joinType = "left_outer").
      filter($"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_13").alias("ESN_13"),
        sum("CONNECTION_13").alias("CON_13")
      )

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP
    val df21 = aggFullDF.join(count21,
      aggFullDF("CALC_DATE") >= count21("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count21("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count21("ENGINE_FAMILY") &&
        aggFullDF("REL_QUARTER_BUILD_DATE") === count21("B_QUARTER") &&
        aggFullDF("BUILD_YEAR") === count21("B_YEAR"), joinType = "left_outer").
      filter($"REL_USER_APPL_DESC" === "All" && $"REL_OEM_NORMALIZED_GROUP" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_21").alias("ESN_21"),
        sum("CONNECTION_21").alias("CON_21")
      )

    // REL_USER_APPL_DESC, REL_QUARTER_BUILD_DATE
    val df22 = aggFullDF.join(count22,
      aggFullDF("CALC_DATE") >= count22("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count22("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count22("ENGINE_FAMILY") &&
        aggFullDF("BUILD_YEAR") === count22("B_YEAR") &&
        aggFullDF("REL_OEM_NORMALIZED_GROUP") === count22("OEM"), joinType = "left_outer").
      filter($"REL_USER_APPL_DESC" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_22").alias("ESN_22"),
        sum("CONNECTION_22").alias("CON_22")
      )

    // REL_OEM_NORMALIZED_GROUP, REL_QUARTER_BUILD_DATE
    val df23 = aggFullDF.join(count23,
      aggFullDF("CALC_DATE") >= count23("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count23("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count23("ENGINE_FAMILY") &&
        aggFullDF("BUILD_YEAR") === count23("B_YEAR") &&
        aggFullDF("REL_USER_APPL_DESC") === count23("USER_APPL"), joinType = "left_outer").
      filter($"REL_OEM_NORMALIZED_GROUP" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_23").alias("ESN_23"),
        sum("CONNECTION_23").alias("CON_23")
      )

    // BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val df24 = aggFullDF.join(count24,
      aggFullDF("CALC_DATE") >= count24("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count24("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count24("ENGINE_FAMILY") &&
        aggFullDF("REL_OEM_NORMALIZED_GROUP") === count24("OEM") &&
        aggFullDF("REL_USER_APPL_DESC") === count24("USER_APPL"), joinType = "left_outer").
      filter($"BUILD_YEAR" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_24").alias("ESN_24"),
        sum("CONNECTION_24").alias("CON_24")
      )

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP, REL_QUARTER_BUILD_DATE
    val df31 = aggFullDF.join(count31,
      aggFullDF("CALC_DATE") >= count31("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count31("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count31("ENGINE_FAMILY") &&
        aggFullDF("BUILD_YEAR") === count31("B_YEAR"), joinType = "left_outer").
      filter($"REL_USER_APPL_DESC" === "All" && $"REL_OEM_NORMALIZED_GROUP" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_31").alias("ESN_31"),
        sum("CONNECTION_31").alias("CON_31")
      )

    // REL_USER_APPL_DESC, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val df32 = aggFullDF.join(count32,
      aggFullDF("CALC_DATE") >= count32("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count32("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count32("ENGINE_FAMILY") &&
        aggFullDF("REL_OEM_NORMALIZED_GROUP") === count32("OEM"), joinType = "left_outer").
      filter($"REL_USER_APPL_DESC" === "All" && $"BUILD_YEAR" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_32").alias("ESN_32"),
        sum("CONNECTION_32").alias("CON_32")
      )

    // REL_OEM_NORMALIZED_GROUP, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val df33 = aggFullDF.join(count33,
      aggFullDF("CALC_DATE") >= count33("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count33("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count33("ENGINE_FAMILY") &&
        aggFullDF("REL_USER_APPL_DESC") === count33("USER_APPL"), joinType = "left_outer").
      filter($"REL_OEM_NORMALIZED_GROUP" === "All" && $"BUILD_YEAR" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_33").alias("ESN_33"),
        sum("CONNECTION_33").alias("CON_33")
      )

    // REL_USER_APPL_DESC, REL_OEM_NORMALIZED_GROUP, BUILD_YEAR, REL_QUARTER_BUILD_DATE
    val df4 = aggFullDF.join(count4,
      aggFullDF("CALC_DATE") >= count4("BUILD_DATE") &&
        aggFullDF("REL_ENGINE_NAME_DESC") === count4("ENGINE_NAME") &&
        aggFullDF("REL_CMP_ENGINE_NAME") === count4("ENGINE_FAMILY"), joinType = "left_outer").
      filter($"REL_USER_APPL_DESC" === "All" && $"REL_OEM_NORMALIZED_GROUP" === "All" && $"BUILD_YEAR" === "All" && $"REL_QUARTER_BUILD_DATE" === "All").
      groupBy("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP", "REL_USER_APPL_DESC").
      agg(
        sum("count_esn_4").alias("ESN_4"),
        sum("CONNECTION_4").alias("CON_4")
      )

    val result = aggFullDF.
      join(df11, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df12, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df13, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df21, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df22, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df23, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df24, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df31, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df32, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df33, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      join(df4, Seq("CALC_DATE", "REL_ENGINE_NAME_DESC", "REL_USER_APPL_DESC", "REL_CMP_ENGINE_NAME", "BUILD_YEAR", "REL_QUARTER_BUILD_DATE", "REL_OEM_NORMALIZED_GROUP"), "left_outer").
      withColumn("BUILD_VOLUME", $"ESN_0" + coalesce($"ESN_11", lit(0)) + coalesce($"ESN_12", lit(0)) + coalesce($"ESN_13", lit(0)) + coalesce($"ESN_21", lit(0)) + coalesce($"ESN_22", lit(0)) + coalesce($"ESN_23", lit(0)) + coalesce($"ESN_24", lit(0)) + coalesce($"ESN_31", lit(0)) + coalesce($"ESN_32", lit(0)) + coalesce($"ESN_33", lit(0)) + coalesce($"ESN_4", lit(0))).
      withColumn("CONNECTION_VOLUME", $"CON_0" + coalesce($"CON_11", lit(0)) + coalesce($"CON_12", lit(0)) + coalesce($"CON_13", lit(0)) + coalesce($"CON_21", lit(0)) + coalesce($"CON_22", lit(0)) + coalesce($"CON_23", lit(0)) + coalesce($"CON_24", lit(0)) + coalesce($"CON_31", lit(0)) + coalesce($"CON_32", lit(0)) + coalesce($"CON_33", lit(0)) + coalesce($"CON_4", lit(0))).
      drop("ESN_0", "ESN_11", "ESN_12", "ESN_13", "ESN_21", "ESN_22", "ESN_23", "ESN_24", "ESN_31", "ESN_32", "ESN_33", "ESN_4").
      drop("CON_0", "CON_11", "CON_12", "CON_13", "CON_21", "CON_22", "CON_23", "CON_24", "CON_31", "CON_32", "CON_33", "CON_4")


    Util.saveData(getResult(result), getTargetDB, cfg(Constants.OM_BV_RESULT) + "_" + getEngType, cfg(Constants.HDFS_PATH))
    logger.info(s"Data getting saved: $getTargetDB ${cfg(Constants.OM_BV_RESULT)}")
  }
  def getResult(result:Dataset[Row]): Dataset[Row] ={
    if(cfg(Constants.DATA_SAMPLE).equalsIgnoreCase("true")){
      logger.warn(s"""WARN :: Saving data WITHOUT 'All' >>>>>>>>>>>>>>>>>>>>>>>>>>>""")
      result
        .where("CALC_DATE != 'All' and REL_USER_APPL_DESC != 'All' and BUILD_YEAR != 'All' and REL_QUARTER_BUILD_DATE != 'All' and REL_OEM_NORMALIZED_GROUP != 'All' ")
    } else
      result
  }
}
