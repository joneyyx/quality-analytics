package mck.qb.library

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import Util._
import org.apache.spark.sql.types._
import org.junit.Test

@Test
class UtilSpec extends FlatSpec with SparkSessionTestWrapper with GivenWhenThen with Matchers {

  case class Eds(EdsId: Int, ESN: Long, DSID: String, FAULTCODE: Int)

  val sourceDF = Seq(
    Eds(1, 11197265, "DS1231", 1234),
    Eds(2, 11197285, "DS1232", 1011),
    Eds(3, 11197295, "DS1233", 1012),
    Eds(4, 11197296, "DS1234", 1013),
    Eds(5, 11197267, "DS1235", 1014)
  )
  val schemaMap = Map(
    "EdsId" -> LongType,
    "ESN" -> LongType,
    "DSID" -> StringType,
    "FAULTCODE" -> IntegerType)

  import spark.implicits._

  "Dataframe" should "be saved" in {

    Given("dataframe")
    Util.saveData(sourceDF.toDF, "test", "test", "/tmp/")

    Given("should be saved to a given path")
    val parQDS = Util.readData("/tmp/test", PARQUET).as[Eds]

    Then("length should be 5")
    parQDS.collect() should have length 5

    Then("should contain element")
    parQDS.collect() should contain (
      Eds(1, 11197265, "DS1231", 1234)
    )

  }

  "Parquet path" should "read parquet into DataFrame" in {
    Given("parquet path")
    val parqDF = readData("/tmp/test", PARQUET)

    Then("should create a DataFrame")
    parqDF.collect() should not be empty
  }

  "Dataframe" should "have an ID column with counter" in {
    Given("dataframe zipWithIndex")

    val dfZipped = dfZipWithIndex(sourceDF.toDF)

    Then("ID column should get added")
    dfZipped.columns should contain ("ID")

    Then("data types should contain an ID LongType")
    dfZipped.dtypes should contain (("ID","LongType"))

    Then("column length now should be 5")
    dfZipped.columns should have length 5

  }

  "Dataframe and map" should "apply schema to the dataframe" in {

    Given("dataframe and a map of schema")
    val appliedDF = applySchema(sourceDF.toDF, schemaMap)

    println(appliedDF.dtypes.toMap.get("EDSID"))

    Then("should apply schema")
    appliedDF.dtypes.toMap.get("EDSID") should be (Some("LongType"))

    Then("schema should be LongType")
    appliedDF.schema.headOption.get.dataType should be (LongType)
  }
}
