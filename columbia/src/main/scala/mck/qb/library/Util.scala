package mck.qb.library

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.constants.Constants.{PATH, _}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
import scala.util.Try

object Util extends Serializable {
  val spark = getSpark()

  def getSpark(): SparkSession = {
    try {
      SparkSession.builder().enableHiveSupport()
        .config("spark.shuffle.service.enabled",true)
        .config("spark.driver.maxResultSize","4G")
        .config("spark.sql.parquet.writeLegacyFormat", true)
        .getOrCreate()
    } catch {
      case e: Exception => {
        SparkSession.builder().
          master("local[2]").
          appName("Spark Locally").
          getOrCreate()
      }
    }
  }

  spark.conf.set("spark.sql.shuffle.partitions", 400)
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def timeToSecondsUdf = udf {
    (unit: String, value: String) =>

      unit match {
        case "HHHHHH:MM:SS" | "HH:MM:SS" => {
          val Array(hour, minute, second) = value.split(":")
          hour.toFloat * 3600.0 + minute.toFloat * 60.0 + second.toFloat
        }

        case _ => 0.0000
      }
  }

  def flattenListListUDF = udf((listList: scala.collection.mutable.WrappedArray[scala.collection.mutable.WrappedArray[Int]]) => {
    listList.filter(_.length != 0).flatten.toList
  })

  def getNthColumnUDF = udf((data: String, n: Integer) => {
    val delimiter: String = " "

    if (data == null) {
      null
    } else {
      val trimmedData = data.trim
      if (trimmedData == "" || trimmedData == "null") {
        null
      } else {
        val split = trimmedData.split(delimiter)
        if (split.length > n) {
          split(n)
        } else {
          null
        }
      }
    }
  })

  def getWarehousePath(warehouseRootPath: String, database: String = "default", tableName: String): String = {
    var dbname = database.toLowerCase
    var layer = "na"
    if (dbname.contains("_raw")) {
      dbname = dbname.replaceAll("_raw", "")
      layer = "raw"
    }
    else if (dbname.contains("_primary")) {
      dbname = dbname.replaceAll("_primary", "")
      layer = "primary"
    }
    else if (dbname.contains("_features") || dbname.contains("_feature")) {
      dbname = dbname.replaceAll("_features", "")
      dbname = dbname.replaceAll("_feature", "")
      layer = "features"
    }
    else if (dbname.contains("_mi") || dbname.contains("_modelinput")) {
      dbname = dbname.replaceAll("_modelinput", "")
      dbname = dbname.replaceAll("_mi", "")
      layer = "mi"
    }
    else if (dbname.contains("_monitoring")) {
      layer = "reference"
    }
    else if (dbname.contains("_mo") || dbname.contains("_modeloutput")) {
      dbname = dbname.replaceAll("_modeloutput", "")
      dbname = dbname.replaceAll("_mo", "")
      layer = "mo"
    }
    else if (dbname.contains("_rpt") || dbname.contains("_report")) {
      dbname = dbname.replaceAll("_report", "")
      dbname = dbname.replaceAll("_rpt", "")
      layer = "rpt"
    }
    else if (dbname.contains("_reference")) {
      dbname = dbname.replaceAll("_reference", "")
      layer = "reference"
    }
    warehouseRootPath.toLowerCase + "/" + layer + "/" + dbname + "/" + tableName.toLowerCase
  }


  def getAllPossibleDataframe(inputdf: DataFrame, colList: Seq[String], combinationCols: Seq[String], Defaultvalue: String = "All"): DataFrame = {

    val comblists = combinationCols.toSet.subsets.map(_.toSeq).toSeq
    val combdfs = comblists.map(combination => combination.foldLeft(inputdf.selectExpr((colList.toSet diff combination.toSet).toSeq: _*)) {
      case (transdf, withcol) => {
        transdf.withColumn(withcol, lit(Defaultvalue))
      }
    }.selectExpr(colList: _*))
    combdfs.reduce((a, b) => a.union(b))

  }

  //  spark.udf.register("your_func_name", getFaultCode, ArrayType(IntegerType()))
  def getFaultCodesUDF = udf((data: String) => {
    if (data == null) {
      null
    } else {
      val intGetterRegex = "\\d+".r
      intGetterRegex.findAllIn(data).matchData.toList.map(_.toString.toInt)
    }
  })

  def profile(dataFrame: DataFrame): Unit = {
    val columnsInDf = dataFrame.columns
    val minCols = columnsInDf.map(col => min(col).as("min_" + col))
    val maxCols = columnsInDf.map(col => max(col).as("max_" + col))
    val meanCols = columnsInDf.map(col => mean(col).as("mean_" + col))
    val distinctValsCols = columnsInDf.map(col => countDistinct(col).as("distinctValues_" + col))
    val allAggCols = minCols ++ maxCols ++ meanCols ++ distinctValsCols

    val countOfRows = dataFrame.count()
    println("total number of rows in data frame: ", countOfRows)
    println("Unique id/ids are the following: ", dataFrame.columns.filter(col => dataFrame.select(col).distinct.count() == countOfRows))
    println("Total number of non-null values in each column: ")
    //    dataFrame.describe().filter($"summary" === "count").show // WHAT
    println("Aggregate statistics of data frame: ")
    dataFrame.select(allAggCols: _*).show()
  }

  //Function to Capitalize and format(remove spaces/spl characters) from column names
  def formatCapitalizeNames(dataFrame: DataFrame): DataFrame = {
    val capitalizedNames = dataFrame.columns.map(colname => colname.replaceAll("[(|)]", "").replaceAll(" ", "_").replaceAll("/", "").replaceAll("\\\\", "").replaceAll("-", "_").toUpperCase)
    val originalNames = dataFrame.columns
    dataFrame.select(List.tabulate(originalNames.length)(i => col(originalNames(i)).as(capitalizedNames(i))): _*)
  }

  def getTimeSeconds = udf {
    (time: Timestamp) => if (time == null) Double.NaN else math.round(time.getTime / 1000.0)
  }

  //function to convert km to miles. (for eg. we use it in Reliability - engine claims)
  def convertKmToMiles = udf {
    (unit: String, mileage: Double) => {
      if (unit == "KM" && mileage != null) Some(mileage * 0.621371)
      else if (unit == "ML" && mileage != null) Some(mileage)
      else None
    }
  }

  def formatCapitalizeNamesWithPrefix(dataFrame: DataFrame, prefix: String): DataFrame = {
    val capitalizedNames = dataFrame.columns.map(colname => prefix + colname.replaceAll("[(|)]", "").replaceAll(" ", "_").toUpperCase)
    val originalNames = dataFrame.columns
    dataFrame.select(List.tabulate(originalNames.length)(i => col(originalNames(i)).as(capitalizedNames(i))): _*)
  }

  def aggCols(dataFrame: DataFrame, groupBy: Seq[String], orderBy: Seq[String], colsToAgg: Seq[String]): DataFrame = {
    val window = Window.partitionBy(groupBy.head, groupBy.tail: _*).orderBy(orderBy.head, orderBy.tail: _*)
    val aggCols = colsToAgg.map(column => max(col(column)).over(window).as("max_" + column)) ++
      colsToAgg.map(column => min(col(column)).over(window).as("min_" + column)) ++
      colsToAgg.map(column => mean(col(column)).over(window).as("mean_" + column)) ++
      colsToAgg.map(column => sum(col(column)).over(window).as("sum_" + column)) ++ Seq(groupBy.map(col)).flatten
    dataFrame.select(aggCols: _*)
  }

  def parseClosureType = udf { s: String => s.split("\\.").last }

  def normalizeYearUDF = udf((year: Int, days: Int) => {
    365 * (year - 2011) + days // erros for leap year... whatever
  })

  def isWeekendUDF = udf((dayOfWeek: String) => {
    dayOfWeek == "Saturday" || dayOfWeek == "Sunday"
  })

  def addMissingColumns(cols: Set[String], fullCols: List[String]): List[Column] = {
    fullCols.map {
      case strCol if cols.contains(strCol) => col(strCol)
      case strCol => lit(null).as(strCol)
    }
  }

  def unionMismatchedDf(leftDf: DataFrame, rightDf: DataFrame): DataFrame = {
    val leftCols = leftDf.columns.toSet
    val rightCols = rightDf.columns.toSet

    val fullCols = leftCols.union(rightCols).toList

    val newLeftCols = addMissingColumns(leftCols, fullCols)
    val newRightCols = addMissingColumns(rightCols, fullCols)

    leftDf.select(newLeftCols: _*).union(
      rightDf.select(newRightCols: _*)
    )
  }

  import spark.implicits._
  //  val df1 = sc.parallelize(List(1)).toDF("a")
  //  val df2 = sc.parallelize(List(2)).toDF("b")
  //  unionMismatchedDf(df1,df2).show()

  def getGWDescUDF = udf(
    (data: String, n: Integer) => {
      val delimiter: String = " "
      if (data == null) {
        null
      } else {
        val trimmedData = data.trim
        if (trimmedData == "" || trimmedData == "null") {
          null
        } else {
          val split = trimmedData.split(delimiter)
          if (split.length > n) {
            split.slice(n, split.length).mkString(" ")
          } else {
            null
          }
        }
      }
    })

  val arrayMax = udf {
    (arr: WrappedArray[Integer]) => {
      arr.max
    }
  }

  /**
    * Essentially a list of lists.  flatten will create a new list of the concatenated lists.
    */
  val flatMapStr = udf {
    (arr: WrappedArray[WrappedArray[String]]) => arr.flatten
  }

  /**
    * Essentially a list of timestamp datatype.  flatten will create a new list of strings.
    */
  val flatTimestampMapString = udf {
    (arr: WrappedArray[Timestamp]) => arr.map(_.toString)
  }
  val flatTimestampMapStrSet = udf {
    (arr: WrappedArray[WrappedArray[Timestamp]]) => arr.flatten.map(_.toString)
  }

  /**
    * Essentially a list of lists.  flatten will create a new list of the concatenated lists with distinct elements.
    * Order is not preserved.
    */
  val flatMapStrSet = udf {
    (arr: WrappedArray[WrappedArray[String]]) => arr.flatten.toSet.toList
  }

  /**
    * Essentially a list of lists.  flatten will create a new list of the concatenated lists.
    */
  val flatMapInt = udf {
    (arr: WrappedArray[WrappedArray[Integer]]) =>
      Try {
        arr.filter(_ != null).flatten
      }.getOrElse(WrappedArray.empty)
  }

  /**
    * Essentially a list of lists.  flatten will create a new list of the concatenated lists.
    */
  val flatMapList = udf {
    (arr: WrappedArray[WrappedArray[Double]]) => arr.flatten
  }

  /**
    * Essentially a list of lists.  flatten will create a new list of the concatenated lists.
    */
  val flatMapDouble = udf {
    (arr: WrappedArray[WrappedArray[Double]]) => arr.flatten
  }

  def daysBetweenDates(start: java.sql.Date, end: java.sql.Date): Long = {
    daysBetween(start.getTime, end.getTime)
  }

  def daysBetween(start: Long, end: Long): Long = {
    scala.math.round((end - start) / 1000.0 / 60.0 / 60.0 / 24)
  }

  def getBeforeDate(maxDate: java.util.Date, days: Int): String = {
    var maxcal: Calendar = Calendar.getInstance()
    maxcal.setTime(maxDate)
    maxcal.add(Calendar.DATE, -days)
    val dateStringBefore48Weeks = maxcal.get(Calendar.YEAR) + "-" + (maxcal.get(Calendar.MONTH) + 1) + "-" + maxcal.get(Calendar.DATE)
    dateStringBefore48Weeks
  }

  val cleanNullEmptyStringsUdf = udf((str: String, itemsToNull: WrappedArray[String]) => {
    if (str == null || itemsToNull.contains(str.trim.toLowerCase)) null else str
  })

  def cleanNullEmptyStringInDf(df: DataFrame, itemsToNull: List[String] = List("null", "nil", "")): DataFrame = {
    var newDf = df
    df.columns.foreach(column => {
      newDf = newDf.withColumn(column, cleanNullEmptyStringsUdf(col(column), array(itemsToNull map lit: _*)))
    })
    newDf
  }

  def cleanNullEmptyStringSubsetInDf(df: DataFrame, columns: List[String], itemsToNull: List[String] = List("null", "nil", "")): DataFrame = {
    var newDf = df
    columns.foreach(column => {
      newDf = newDf.withColumn(column, cleanNullEmptyStringsUdf(newDf(column), array(itemsToNull map lit: _*)))
    })
    newDf
  }

  def cleanDfColumns(df: DataFrame, colList: List[String], regex: String, replaceWith: String = ""): DataFrame = {
    var newDf = df
    colList.foreach(c => {
      newDf = newDf.withColumn(c, regexp_replace(col(c), lit(regex), lit(replaceWith)))
    })
    newDf
  }

  def printNiceSelectStatement(df: DataFrame): Unit = {
    df.columns.foreach(column => {
      println("\"" + column + "\",")
    })
  }

  def applySchema(df: DataFrame, schemaMap: Map[String, DataType]) = {
    val upperSchemaMap = schemaMap map { case (k, v) => (k.toUpperCase, v) }
    df.schema.foldLeft(df) {
      case (acc, col) => {
        val colNameUpper = col.name.toUpperCase
        acc.withColumn(colNameUpper, df(col.name).cast(upperSchemaMap getOrElse(colNameUpper, StringType)))
      }
    }
  }

  def reportingESNforOMjobs(df: DataFrame): Unit = {
    var outputDF = df.filter($"CODE" =!= "NA").
      groupBy("CALC_DATE").
      agg(org.apache.spark.sql.functions.countDistinct($"ESN") as "count").
      orderBy(desc("CALC_DATE"))

    outputDF.show(20)
  }

  def reportingCodesforOMjobs(df: DataFrame): Unit = {
    var outputDF = df.filter($"CODE" =!= "NA").
      groupBy("CALC_DATE").
      agg(org.apache.spark.sql.functions.countDistinct($"CODE") as "count").
      orderBy(desc("CALC_DATE"))

    outputDF.show(20)
  }

  def cleanSequencedColumns(df: DataFrame, startIndex: Int, endIndex: Int, prefix: String, pattern: String, replacement: String): DataFrame = {
    var outputDf = df
    (startIndex until endIndex).foreach(index => {
      outputDf = outputDf.
        withColumn(s"${prefix}${index}", regexp_replace(col(s"${prefix}${index}"), pattern, replacement))
    })
    outputDf
  }

  def reduceByKey(collection: Traversable[Tuple2[String, Double]]) = {
    collection
      .groupBy(_._1)
      .map { case (group: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }
  }

  val populateIfTrue = udf(
    (condition: Boolean, string: String) => {
      if (condition) string else null
    }
  )

  val median = udf((values: mutable.WrappedArray[Double]) => {
    val list = values.toList.sorted
    if (list.size == 0) {
      null.asInstanceOf[Double]
    } else if (list.size % 2 == 1) {
      list((list.size - 1) / 2).toDouble
    } else {
      (list(list.size / 2) + list(list.size / 2 - 1)) / 2.0
    }
  })

  def unitConverter = udf {
    (unit: String, value_str: String) =>

      var value = ""

      if (!value_str.equalsIgnoreCase("NULL")) {

        if (value_str.toString.contains(",")) {
          value = value_str.toString.replaceAll(",", ".")
        } else {
          value = value_str
        }

        unit.toUpperCase() match {
          case "GAL" =>
            if (value.toString.contains(",")) {
              value.toString.replaceAll(",", ".")
            } else {
              value
            }

          case "MI" | "MPH" =>
            if (value.toString.contains(",")) {
              value.toString.replaceAll(",", ".")
            } else {
              value
            }

          //Convert KM to Miles
          case "KM" | "KM/HR" =>
            if (value.toString.contains(",")) {
              (value.toString.replaceAll(",", ".").toFloat * 0.6213).toString

            } else {
              (value.toFloat * 0.6213).toString

            }

          // Convert L to Gal
          case "L" =>
            if (value.toString.contains(",")) {
              (value.toString.replaceAll(",", ".").toFloat * 0.2641).toString
            } else {
              (value.toFloat * 0.2641).toString
            }

          case "IGAL" =>
            if (value.toString.contains(",")) {
              (value.toString.replaceAll(",", ".").toFloat * 1.2009).toString
            } else {
              (value.toFloat * 1.2009).toString
            }
          //Convert Celsius to Fahrenheit
          case "°C" | "DEG_C" => ((value.toDouble * 9) / 5 + 32).toString

          case "°F" => value

          /** KPA to PSI conversion
            * KPA_G is gauge pressure
            * KPA_A is absolute pressure
            * All Units converted to PSI
            * */
          case "KPA" | "KPA_G" | "KPA_A" => (value.toDouble * 0.145).toString

          /*
          * BAR to PSI conversion
          * BAR_A is absolute pressure.
          * All units converted to PSI
          * */
          case "BAR" | "BAR_A" => (value.toDouble * 14.50).toString

          // Change inHG (inch of merquery )to PSI
          case "INHG" => (value.toDouble * 0.49).toString

          // Change inH2O (inch of water) to PSI
          case "INH2O" => (value.toDouble * 0.036).toString

          case "PSI" => value

          case "KRPM" => (value.toDouble / 1000).toString

          case "RPM" => value

          //FT3/S to M3/S conversion
          case "FT3/S" => (value.toDouble * 101.94).toString
          // M3/S no conversion required
          case "M3/S" => value

          case "%" => value
          // volts
          case "V" => value

          case "°" => value

          case "PERCENT" => value

          case "REVS/MI" => value

          case "LBF*FT/S" => value

          //Convert TIME to HOURS
          case "SECOND" | "S" => (value.toDouble / 3600).toString

          case "PPM" => value

          // Convert TIME to HOURS
          case "HHHHHH:MM:SS" | "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
               "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M" =>
            val Array(hour, min, sec) = value.split(":")
            (hour.toInt + min.toFloat / 60.0).toString

          // Convert Kilowatts(kW) to electrical horsepower (hp)
          // One electrical horsepower is equal to 0.746 kilowatts, so formula P(hp) = P(kW) / 0.746
          case "KW" => (value.toDouble / 0.746).toString

          case "HP" => value

          // Convert KM/L to miles/gal
          case "KM/L" => (value.toDouble / 2.35214583).toString

          case "NA" =>
            if (value.toString.contains(",")) {
              value.toString.replaceAll(",", ".")
            } else {
              value
            }
          case _ => value
        }
      } else {
        "NULL"
      }
  }

  def getConvertedUnit = udf {
    (unit: String) =>

      unit.toUpperCase match {

        case "KM" => "MI"
        case "KM/HR" => "MPH"
        case "L" | "IGAL" => "GAL"
        case "°C" | "DEG_C" | "°F" => "F"
        case "KPA" | "KPA_G" | "KPA_A" | "BAR" | "BAR_A" | "INHG" | "INH2O" | "PSI" => "PSI"
        case "KRPM" | "RPM" => "RPM"
        case "FT3/S" | "M3/S" => "M3PS"
        case "%" | "PERCENT" => "PERCENT"
        case "V" => "VOLTS"
        case "°" => "DEGREE"
        case "REVS/MI" => "REVSPMI"
        case "LBF*FT/S" => "FOOTPOUNDFORCE"
        case "SECOND" | "S" => "HOURS"
        case "HHHHHH:MM:SS" | "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
             "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M" => "HOURS"
        case "PPM" => "PPM"
        case "KW | HP" => "HP"
        case "KM/L" => "MPG"
        case "NULL" | "NNNN" => "NA"
        case _ => unit
      }
  }

  def getConvertedUnitMetrics(unit: String): String = {
    // CHANGE UNIT NAME TO METRIC FORMAT
    unit.toUpperCase match {
      case "MI" => "KM"
      case "MPH" => "KMH"
      case "GAL" | "IGAL" => "L"
      case "MI/GAL" => "KMPL"
      case "KPA" | "KPA_G" | "KPA_A" | "BAR" | "BAR_A" | "INHG" | "INH2O" | "PSI" => "KPA"
      case "KRPM" | "RPM" => "RPM"
      case "FT3/S" | "M3/S" => "M3PS"
      case "%" | "PERCENT" => "PERCENT"
      case "V" => "VOLTS"
      case "°" => "DEGREE"
      case "REVS/MI" => "REVSPMIN"
      case "LBF*FT/S" => "FOOTPOUNDFORCE"
      case "SECOND" | "S" => "HOURS"
      case "HHHHHH:MM:SS" | "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
           "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M" => "HOURS"
      case "PPM" => "PPM"
      case "KW | HP" => "HP"
      case "FT*LB" | "KGF*M" | "N*M" | "N_M" => "NM"

      case "NULL" | "NNNN" => "NA"
      case _ => unit.toUpperCase
    }
  }

  def unitConverterMetrics = udf {
    // CONVERT ALL THE UNITS TO METRICS FORMAT
    (unit: String, value_str: String) =>
      var value = ""

      if (!value_str.equalsIgnoreCase("NULL")) {

        if (value_str.toString.contains(",")) {
          value = value_str.toString.replaceAll(",", ".")
        } else {
          value = value_str
        }

        unit.toUpperCase() match {
          // CONVERT MI TO KM
          case "MI" | "MPH" => (value.toDouble * 1.6).toString
          // CONVERT GAL TO L
          case "GAL" => (value.toDouble * 3.785).toString
          // CONVERT IGAL TO L
          // IGAL => GAL => L
          case "IGAL" => (value.toDouble * 1.2009 * 3.785).toString

          // CONVERT MI/GAL TO KM/L
          case "MI/GAL" => (value.toDouble * 0.425).toString

          // CONVERT DEG F TO DEG C
          case "°F" | "DEG_F" => ((value.toDouble - 32) / 1.8).toString

          // CONVERT KPA, KPA_A, KPA_G TO KPA

          // CONVERT BAR, BAR_A TO KPA
          case "BAR" | "BAR_A" => (value.toDouble * 100).toString

          // CONVERT INHG TO KPA
          case "INHG" => (value.toDouble * 3.386).toString

          // CONVERT INH2O TO KPA
          case "INH2O" => (value.toDouble * 0.2490).toString

          // CONVERT PSI TO KPA
          case "PSI" => (value.toDouble * 6.894).toString

          //FT3/S to M3/S conversion
          case "FT3/S" => (value.toDouble * 101.94).toString
          // M3/S no conversion required
          case "M3/S" => value

          case "%" => value
          // volts
          case "V" => value

          case "°" => value

          case "PERCENT" => value

          case "REVS/MI" => value

          case "LBF*FT/S" => value

          //Convert TIME to HOURS
          case "SECOND" | "S" => (value.toDouble / 3600).toString

          case "PPM" => value

          // Convert TIME to HOURS
          case "HHHHHH:MM:SS" | "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
               "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M" =>
            val Array(hour, min, sec) = value.split(":")
            (hour.toInt + min.toDouble / 60.0).toString

          // Convert Kilowatts(kW) to electrical horsepower (hp)
          // One electrical horsepower is equal to 0.746 kilowatts, so formula P(hp) = P(kW) / 0.746
          case "KW" => (value.toDouble / 0.746).toString

          case "HP" => value

          // Convert KM/L to miles/gal
          case "KM/L" => value

          case "FT*LB" => (value.toDouble * 1.36).toString
          case "N*M" | "N_M" => value
          case "KGF*M" => (value.toDouble * 9.807).toString

          case "NA" =>
            if (value.contains(",")) {
              value.replaceAll(",", ".")
            } else {
              value
            }
          case _ => value

        }
      } else {
        "NULL"
      }

  }

  def dfZipWithIndex(
                      df: DataFrame,
                      offset: Int = 0,
                      colName: String = "ID",
                      inFront: Boolean = true
                    ): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))
      )
    )
  }

  /**
    * Saves data frame to target table.
    *
    * @param df     data to save
    * @param db     database name
    * @param tbl    table name
    * @param path   warehouse path
    * @param mode   write option mode, default = overwrite
    * @param format write format, default = parquet
    */
  def saveData(df: DataFrame, db: String, tbl: String, path: String, mode: String = "overwrite", format: String = "parquet"): Unit = {
    formatCapitalizeNames(df).write.
      options(Map(Constants.PATH -> Util.getWarehousePath(path, db, tbl))).
      mode(mode).format(format).
      saveAsTable(s"$db.$tbl")
  }

  /**
    * Save the DataFrame as an external table with partitions,
    * the Currying Function can change the 'mode' and 'format' in the first passing parameters
    * @param df         data to save
    * @param db         database name
    * @param tbl        table name
    * @param path       warehouse path
    * @param mode       write option mode, default = overwrite
    * @param format     write format, default = parquet
    * @param partition  Partition field, can be one or more
    */
  def saveDataWithPartition(df: DataFrame,
                            db: String,
                            tbl: String,
                            path: String,
                            mode: String = "overwrite",
                            format: String = "parquet")
                           (
                             partition: String*
                           ): Unit = {
    formatCapitalizeNames(df).write.
      options(Map(path -> Util.getWarehousePath(path, db, tbl)))
      .mode(mode)
      .partitionBy(partition: _*)
      .format(format)
      .saveAsTable(s"$db.$tbl")
  }


  sealed trait FileType

  case object PARQUET extends FileType

  case object CSV extends FileType

  case object JSON extends FileType

  /**
    * Reads data from a given path
    *
    * @param path     path of the file
    * @param fileType PARQUET, CSV
    */
  def readData(path: String, fileType: FileType): DataFrame = {
    try {
      fileType match {
        case PARQUET => spark.read.parquet(path)
        case CSV => spark.read.csv(path)
        case JSON => spark.read.json(path)
        case _ => Seq.empty[String].toDF
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Seq.empty[String].toDF
    }
  }

  /**
    * Returns a warehouse path map.
    *
    * @param db  Database name
    * @param tbl Table name
    */
  def getPath(warehousePath: String, db: String, tbl: String): Map[String, String] = {
    Map(PATH -> Util.getWarehousePath(warehousePath, db, tbl))
  }

  /**
    * Filter a data frame by the specified engine type
    *
    * @param cfg        job configuntitled:Untitled-1
    * @param df         data frame to filter
    * @param engType    engine type (from ref list in DB)
    * @param esnColName ESN-column in the passed in data frame
    * @return data frame with ESNs of the speficied type
    */
  def filterEnginesByType(cfg: String => String, df: DataFrame, engType: String, esnColName: String = "ESN"): DataFrame = {
    val esnsDf: DataFrame = getEngineTypeESNs(engType, cfg)
    df.join(esnsDf, df(esnColName) === esnsDf("filter_esn")).drop("filter_esn")
  }

  /**
    * Return ESN for specified engine type
    *
    * @param engType engine type (from ref list in DB)
    * @param cfg     job config
    * @return data frame with "filter_esn" column with ESNs of specified engine type
    */
  def getEngineTypeESNs(engType: String, cfg: String => String): DataFrame = {
    spark.sql(
      s"""SELECT DISTINCT ENGINE_DETAILS_ESN AS FILTER_ESN
         | FROM ${cfg(FET_DB)}.${cfg(REL_ENG_FET_TBL)} T
         | WHERE T.REL_CMP_ENGINE_NAME = '$engType'
      """.stripMargin)
  }

  /**
    * Return a new Dataset merged "histry_df" and "new_df".
    * If a piece of data is changed it will be replaced.
    * Require historical data to establish "boundaries"
    *
    * @param histry_df  The historical DataFrame
    * @param new_df     New full data
    * @param boundary Time column that will be automatically added
    * @return
    */
  def merge(histry_df: DataFrame, new_df: DataFrame, boundary: String = "update_date"): Dataset[Row] = {
    val histry_withoutdate: DataFrame = histry_df.drop(boundary)
    val IncrementalData: Dataset[Row] = new_df.except(histry_withoutdate)
    val old_data: DataFrame = new_df.join(histry_df, new_df.columns, "inner")
    old_data.union(IncrementalData.withColumn(boundary, from_unixtime(unix_timestamp(), "yyyy-MM-dd HH:mm:ss")))
  }

  def findTheIncremental(histry_df: DataFrame, new_df: DataFrame) = {
    new_df.except(histry_df)
  }



  //////////////////////////////
  // UDF
  //////////////////////////////

  def getNormCoverage = udf {
    coverage: String => {
      if (coverage != null) {
        coverage match {
          case "正常索赔 Warranty" => "Warranty"
          case "政策性索赔 Policy" => "Policy"
          case "主动召回或维修 Campaign /TRP" => "Campaign_TRP"
          case "路试 Field Test" => "Field_test"
          case "Emission" | "排放质保 EMISSIONS" => "Emission"
          case "BIS" => "BIS"
          case _ => "Others"
        }
      } else {
        "Others"
      }
    }
  }

  def getNormbaseats = udf {

    Base_or_ATS: String => {
      if (Base_or_ATS != null) {
        Base_or_ATS.toUpperCase match {
          case "Base" | "BASE" => "BASE"
          case "ATS" => "ATS"
          case _ => Base_or_ATS.toString
        }
      } else {
        null
      }
    }
  }

  def getNormEmission: UserDefinedFunction = udf {

    REL_EMISSION_LEVEL: String => {
      if (REL_EMISSION_LEVEL != null) {
        REL_EMISSION_LEVEL.toUpperCase match {
          case "E5" | "NS5" | "S5" => "NSV"
          case "NS6" => "NSVI"
          case "CS3" => "CSIII"
          case "CS4" => "CSIV"
          case _ => "Others"
        }
      }
      else {
        null
      }
    }
  }

  def getNormDesc: UserDefinedFunction = udf {
    (REL_ENGINE_PLATFORM: String) => {
      if (REL_ENGINE_PLATFORM != null) {
        REL_ENGINE_PLATFORM.toUpperCase match {
          case "ISB4.5" | "ISB5.9" | "B6.2" | "QSB7" | "B4.5" | "B4.0" | "B5.9" | "B6.7" => "B"
          case "ISF2.8" | "ISF3.8" | "ISF4.5" | "F2.8" | "F3.8" | "F4.5" => "F"
          case "ISZ13" | "Z14" | "Z15" => "Z"
          case "L9" | "ISL8.9" | "ISL9.5" | "L8.9" => "L"
          case "ISD4.5" | "ISD6.7" | "D4.0" | "D4.5" | "D6.7" | "ISD4" | "ISD5.9" | "ISD6.2" => "D"
          case "X" | "X11" | "X12" | "X12-XCEC" | "X13" => "X"
          case "DNG6.7" | "LNG8.9" => "NG"
          case "ISM10.8" => "M"
          case _ => REL_ENGINE_PLATFORM.toString
        }
      } else {
        null
      }
    }
  }

  /**
    * REL_ENGINE_PLATFORM	         REL_USER_APPL_DESC 	USER_APPL_TYPE
    * X11/X12/X13/Z14/L9		                              HEAVY_TRUCK
    * X11/X12/X13/Z14/L9	                Dumper	        DUMPER
    * D6.7/B6.2		                                        MEDIUM_TRUCK
    * F2.8/F3.8/F4.5/B4.0/B4.5/D4.0/D4.5		              LIGHT_TRUCK
    *
    * @return
    */
  def getUserAppType: UserDefinedFunction = udf {
    (REL_ENGINE_PLATFORM: String, REL_USER_APPL_DESC: String) => {
      val rep = if (REL_ENGINE_PLATFORM.isEmpty) "" else REL_ENGINE_PLATFORM.toUpperCase
      val ruad = if (REL_USER_APPL_DESC.isEmpty) "" else REL_USER_APPL_DESC.toUpperCase

      (rep, ruad) match {
        case ("X11" | "X12" | "X13" | "Z14" | "L9" | "X12-XCEC", "DUMPER")    => "DUMPER"
        case ("X11" | "X12" | "X13" | "Z14" | "L9" | "X12-XCEC", _)           => "HEAVY_TRUCK"
        case ("D6.7" | "B6.2", _)                                => "MEDIUM_TRUCK"
        case ("F2.8" | "F3.8" | "F4.5" | "B4.0" | "B4.5" | "D4.0" | "D4.5", _) => "LIGHT_TRUCK"
        case _ => REL_USER_APPL_DESC.toUpperCase
      }

    }
  }

  /**
    * REL_ENGINE_PLATFORM	        REL_USER_APPL_DESC	    USER_APPL_CODE
    * X11/X12/X13/Z14/L9		                                Truck
    * X11/X12/X13/Z14/L9	              Dumper	            Dumper
    * D6.7/B6.2		                                          Truck
    * F2.8/F3.8/F4.5/B4.0/B4.5/D4.0/D4.5		                Truck
    *
    * @return
    */
  def getUserAppCode: UserDefinedFunction = udf {
    (REL_ENGINE_PLATFORM: String, REL_USER_APPL_DESC: String) => {
      val rep = if (REL_ENGINE_PLATFORM.isEmpty) "" else REL_ENGINE_PLATFORM.toUpperCase
      val ruad = if (REL_USER_APPL_DESC.isEmpty) "" else REL_USER_APPL_DESC.toUpperCase

      (rep, ruad) match {
        case ("X11" | "X12" | "X13" | "Z14" | "L9" | "X12-XCEC", "DUMPER")    => "Dumper"
        case ("X11" | "X12" | "X13" | "Z14" | "L9" | "X12-XCEC", _)           => "Truck"
        case ("D6.7" | "B6.2", _)                                => "Truck"
        case ("F2.8" | "F3.8" | "F4.5" | "B4.0" | "B4.5" | "D4.0" | "D4.5", _) => "Truck"
        case _ => REL_USER_APPL_DESC.toUpperCase
      }

    }
  }




  //This function takes a string and based on the case statement returns a OEM group name.
  def getNormOEMGrp: UserDefinedFunction = udf {

    REL_OEM_GROUP: String => {
      if (REL_OEM_GROUP != null) {
        REL_OEM_GROUP.toUpperCase match {
          case "东风商用车有限公司" | "东风汽车" => "DFCV"
          case "东风商用车有限公司东风专用汽车底盘公司" => "DFZD"
          case "东风汽车股份有限公司" => "DFAC"
          case "东风柳州汽车有限公司" => "DFLZ"
          case "福田戴姆勒" | "北京福田戴姆勒汽车有限公司" => "BFDA"
          case "福田欧马可汽车厂" => "Foton_Aumark"
          case "福田奥铃汽车厂" => "Foton_Olin"
          case "福田M4工厂" => "Foton_M4"
          case "时代汽车" => "Foton_Forland"
          case "江淮汽车" => "JAC"
          case "重汽" => "CNHTC"
          case "陕西重型汽车有限公司" => "SQ"
          case _ => "OTHERS"
        }
      } else {
        "OTHERS"
      }
    }
  }

  /**
    * Convert Greenwich Mean Time to Beijing Standard Time
    *
    * @return Time UTC+8
    */
  def datetimeUTC2CST = udf {
    (utcStr: String) => {
      if (utcStr == null) {
        "1900-01-01 00:00:00"
      } else if (utcStr.isEmpty) {
        "1900-01-01 00:00:00"
      } else {
        val formatUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val formatCST = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        formatCST.format(formatUTC.parse(utcStr).getTime + 28800000)
      }
    }
  }

  /**
    * Trim non-empty and not null string
    * @return trimed String
    */
  def trimString: UserDefinedFunction =udf{
    str:String=>{
      if (str!=null&&str.nonEmpty) str.trim
      else ""
    }
  }

  /**
    * Select the last element in the List and return null if it is an empty List
    */
  val selectLast: UserDefinedFunction = udf {
    arr: mutable.WrappedArray[String] =>{
      if(arr.isEmpty){
        null
      }else
      arr.last
    }
  }

  /**
    * Example:
    *   before : ("a", "b", "c", "a", "d", "c", "a")
    *   after :  (b * 1, d * 1, a * 3, c * 2)
    */
  val ListReduceCount: UserDefinedFunction = udf {
    arr: mutable.WrappedArray[String] =>{
      arr.map((_: String, 1))
        .groupBy((_: (String, Int))._1)
        .map((x: (String, mutable.WrappedArray[(String, Int)])) => {(x._1, x._2.size)})
        .mkString("\u0001")
        .replaceAll("->","*")
        .split("\u0001")
        .toList
    }
  }

  /**
    *  reduce the list and return String
    *  before : ("a", "b", "c", "a", "d", "c", "a")
    *  after : b * 1,d * 1,a * 3,c * 2
    *
    * @return String
    */
  def ListReduceCount2String = udf((arr: mutable.WrappedArray[String],cnctor:String,sep:String) => {
    arr.map((_: String, 1))
      .groupBy((_: (String, Int))._1)
      .map((x: (String, mutable.WrappedArray[(String, Int)])) => {(x._1, x._2.size)})
      .mkString(sep)
      .replaceAll("->",cnctor)
  })
  def intListToSet = udf((list: scala.collection.mutable.WrappedArray[Int]) => if (list == null) null else list.toSet.toList.sorted)
  def doubleListToSet = udf((list: scala.collection.mutable.WrappedArray[Double]) => if (list == null) null else list.toSet.toList.sorted)
  def stringListToSet = udf((list: scala.collection.mutable.WrappedArray[String]) => if (list == null) null else list.toSet.toList.sorted)
  def longListToSet = udf((list: scala.collection.mutable.WrappedArray[Long]) => if (list == null) null else list.toSet.toList.sorted)

  /**
    * Remove left and right spaces, wraps, tabs
    * @return String
    */
  def removeTabWrapAndTrim = udf{
    str:String=>{
      if (str!=null&&str.nonEmpty)
      str.trim.replaceAll("\\t|\\r|\\n","")
      else
        str
    }
  }

  /**
    * Intercept the string according to the specified cut character
    * (first index)
    * @return
    */
  def subStringWithSpec = udf{
    (str:String,spec:String)=>{
      if (str!=null&&str.nonEmpty)
        str.substring(0,str.indexOf(spec))
      else
        str
    }
  }

  /**
    * Convert string to uppercase
    * @return
    */
  def toUpperCase: UserDefinedFunction = udf {

    (str: String) => {
      str.toUpperCase
    }
  }


  val splitString2Set = udf((str:String) => {

      if (str != null && str.trim.nonEmpty)
        str.replaceAll("，",",").split(",")
      else
        null
    })

  /**
    * Calculate months between two build_date and failure_date
    * algorithm: (failure_year - build_year) * 12 + filure_month - build_month
    * Ex1: failure_date = 2019-01-01, build_date = 2018-12-01 => 1
    * Ex2: failure_date = 2019-03-15, build_date = 2019-03-01 => 0
    * Ex3: failure_date = 2019-02-01, build_date = 2017-11-15 => 15
    * @return integer
    */
  def monthsBetween = udf{
    (failure_date: Date, build_year: Integer, build_month: Integer) => {
      val failure_year = failure_date.toLocalDate.getYear
      val failure_month = failure_date.toLocalDate.getMonthValue
      (failure_year - build_year) * 12 + failure_month - build_month
    }
  }

  def pickUp = udf(
    (a:mutable.WrappedArray[Long],b:mutable.WrappedArray[Long])=>{
      a.toList ::: b.toList
    }
  )

  def pickUpString = udf(
    (a:mutable.WrappedArray[String],b:mutable.WrappedArray[String])=>{
      a.toList ::: b.toList
    }
  )

  def getMost: UserDefinedFunction = udf(
    (arr: mutable.WrappedArray[String]) => {
      val reverse: List[(String, Int)] =
        arr.filter((x: String) =>{
          x.nonEmpty && x != null && x!= " "
        } ).map((_: String, 1)).groupBy((x: (String, Int)) => x._1).mapValues((_: mutable.WrappedArray[(String, Int)]).size).toList.sortBy((_: (String, Int))._2).reverse
      reverse.head._1
    }
  )
}
