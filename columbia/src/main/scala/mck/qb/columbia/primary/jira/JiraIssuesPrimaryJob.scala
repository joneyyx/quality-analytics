package mck.qb.columbia.primary.jira

import com.typesafe.config.Config
import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.constants.Constants
import mck.qb.library.{Job, Util}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}

/**
  * Created by newforesee on 2020/5/22
  */
object JiraIssuesPrimaryJob extends Job {

  import spark.implicits._

  val myConf: Config = AppConfig.getMyConfig(Constants.PRI_JIRA_ISUS)
  logger.warn("Config utilized - (%s)".format(myConf))
  val cfg: String => String = myConf.getString

  val JSON_PATH: String = Util.getWarehousePath(cfg(Constants.HDFS_PATH), cfg(Constants.RAW_DB), cfg(Constants.JIRA_ISUS_RAW_TBL))

  override def run(): Unit = {


    val schema = new StructType().
      add("startAt", "string", true).
      add("maxResults", "string", true).
      add("total", "string", true).
      add("issues",
        ArrayType(new StructType().
          add("expand", "string", true).
          add("id", "string", true).
          add("self", "string", true).
          add("key", "string", true).
          add("fields",
            new StructType().
              add("assignee",
                new StructType().
                  add("name", "string", true).
                  add("emailAddress", "string", true).
                  add("displayName", "string", true).
                  add("active", "boolean", true)).
              add("subtasks",
                ArrayType(new StructType())).
              add("reporter", new StructType().
                add("name", "string", true).
                add("emailAddress", "string", true).
                add("displayName", "string", true).
                add("active", "boolean", true)).
              add("issuetype", new StructType().
                add("name", "string", true).
                add("subtask", "boolean", true)).
              add("project", new StructType().
                add("id", "string", true).
                add("key", "string", true).
                add("name", "string", true)).
              add("customfield_10036", "string", true).
              add("customfield_10037", "string", true).
              add("updated", "string", true).
              add("description", "string", true).
              add("summary", "string", true).
              add("duedate", "string", true).
              add("customfield_10113", "string", true).
              add("priority", new StructType().
                add("name", "string", true)).
              add("status", new StructType().
                add("name", "string", true).
                add("statusCategory", new StructType().
                  add("colorName", "string", true).
                  add("name", "string", true))).
              add("created", "string", true).
              add("customfield_10090", new StructType().
                add("value", "string", true)).
              add("customfield_10093", new ArrayType(StringType, true)).
              add("customfield_10107", "string", true).
              add("customfield_10092", new ArrayType(StringType, true)).
              add("customfield_10251", new ArrayType(MapType(StringType, StringType), true)).
              add("customfield_10112", new ArrayType(MapType(StringType, StringType), true)).
              add("parent", new StructType().add("id", "string", true))

          )))
    spark.read.schema(schema).json(s"$JSON_PATH/").createOrReplaceTempView("temp")
    //    spark.sql("select * from temp").show()

    val jsondf = spark.sql(
      """SELECT
        |ISSUES.ID AS Id,
        |ISSUES.SELF AS Self,
        |ISSUES.KEY AS Key,
        |ISSUES.FIELDS.ASSIGNEE.NAME AS Assignee_Name,
        |ISSUES.FIELDS.ASSIGNEE.EMAILADDRESS AS Assignee_Emailaddress,
        |ISSUES.FIELDS.ASSIGNEE.DISPLAYNAME AS Assignee,
        |ISSUES.FIELDS.ASSIGNEE.ACTIVE AS Assignee_Active,
        |ISSUES.FIELDS.REPORTER.NAME AS Reporter_Name,
        |ISSUES.FIELDS.REPORTER.EMAILADDRESS AS Reporter_Emailaddress,
        |ISSUES.FIELDS.REPORTER.DISPLAYNAME AS Reporter_Displayname,
        |ISSUES.FIELDS.REPORTER.ACTIVE AS Reporter_Active,
        |ISSUES.FIELDS.ISSUETYPE.NAME AS Issuetype,
        |ISSUES.FIELDS.ISSUETYPE.SUBTASK AS Issubtask,
        |ISSUES.FIELDS.PROJECT.ID AS Project_Id,
        |ISSUES.FIELDS.PROJECT.KEY AS Project_Key,
        |ISSUES.FIELDS.PROJECT.NAME AS Project_Name,
        |ISSUES.FIELDS.CUSTOMFIELD_10036 AS Raisedate,
        |ISSUES.FIELDS.CUSTOMFIELD_10037 AS Dispositiondate,
        |ISSUES.FIELDS.UPDATED AS Updated,
        |ISSUES.FIELDS.DESCRIPTION AS Description,
        |ISSUES.FIELDS.SUMMARY AS Summary,
        |ISSUES.FIELDS.DUEDATE AS Duedate,
        |ISSUES.FIELDS.CUSTOMFIELD_10113 AS EscalationID,
        |ISSUES.FIELDS.PRIORITY.NAME AS Priority,
        |ISSUES.FIELDS.STATUS.NAME AS Status_Name,
        |ISSUES.FIELDS.STATUS.STATUSCATEGORY.NAME AS Status_Statuscategory_Name,
        |ISSUES.FIELDS.CREATED AS Created,
        |ISSUES.FIELDS.CUSTOMFIELD_10090.VALUE AS Signalsource,
        |ISSUES.FIELDS.CUSTOMFIELD_10093 AS Customfield_10093,
        |ISSUES.FIELDS.CUSTOMFIELD_10092 AS Customfield_10092,
        |ISSUES.FIELDS.PARENT.ID AS Parent_Id,
        |ISSUES.FIELDS.customfield_10251 AS customfield_10251,
        |ISSUES.FIELDS.customfield_10112 AS customfield_10112,
        |ISSUES.FIELDS.customfield_10107 AS pps_id
        | FROM (SELECT STARTAT,
        |MAXRESULTS,
        |TOTAL,
        |EXPLODE(ISSUES) AS ISSUES
        |FROM TEMP
        |)""".stripMargin)
    val formatdf = jsondf.where("""Id is not null and id<>""""")
      .withColumn("Raisedate", to_date($"Raisedate"))
      .withColumn("Dispositiondate", to_date($"Dispositiondate"))
      .withColumn("Created", to_date($"created".substr(1, 10)))
      .withColumn("Updated", to_date($"updated".substr(1, 10)))
      .withColumn("Duedate", to_date($"duedate"))
      .withColumnRenamed("CUSTOMFIELD_10093", "Fail_Codes")
      .withColumnRenamed("CUSTOMFIELD_10092", "Fault_Codes")
      .withColumnRenamed("self", "Jira_Link")
      .withColumn("Assignee_Active", when($"Assignee_Active" === true, "1").otherwise("0"))
      .withColumn("Reporter_Active", when($"Reporter_Active" === true, "1").otherwise("0"))
      .withColumn("Issubtask", when($"Issubtask" === true, "1").otherwise("0"))


    val fail_null_df = formatdf.filter($"Fail_Codes".isNull).withColumn("Fail_Codes", $"Fail_Codes".cast(StringType))
    val fail_df = formatdf.filter($"Fail_Codes".isNotNull).
      withColumn("Fail_Codes", explode($"Fail_Codes")).union(fail_null_df)
    val fault_null_df = fail_df.filter($"Fault_Codes".isNull).withColumn("Fault_Codes", $"Fault_Codes".cast(StringType))
    val fault_df = fail_df.filter($"Fault_Codes".isNotNull).
      withColumn("Fault_Codes", explode($"Fault_Codes")).union(fault_null_df)
    val explodedf = fault_df
    val resdf = explodedf.
      withColumn("CODE", addCodeWithCondition($"Fail_Codes", $"Fault_Codes"))
      .withColumn("Description", regexp_replace($"Description", "\\s+", ";"))
      .withColumn("Jira_Link", concat(lit("https://cumminsit.atlassian.net/browse/"), $"Key"))
      .withColumn("Platform", $"customfield_10251"(0))
      .withColumn("Platform", $"Platform.value")
      .withColumn("Entity", $"customfield_10112"(0))
      .withColumn("Entity", coalesce($"Entity.value", substring_index($"Project_Name", "-", 1)))
      .drop("customfield_10251", "customfield_10112")


    Util.saveData(resdf,cfg(Constants.PRI_DB),cfg(Constants.JIRA_ISUS_DTL_TBL),cfg(Constants.HDFS_PATH))



//    resdf
//      .coalesce(1)
//      .write
//      .mode("overwrite")
//      .option("sep", "`")
//      .option("header", true)
//      .csv("/Users/newforesee/IdeaProjects/spark/src/main/scala/newforesee/selenium/csv")


  }

  def addCodeWithCondition = udf {
    (str1: String, str2: String) => {
      if (str1 != null && str1.nonEmpty) str1.trim
      else str2
    }
  }


  //  def getMapIterm = udf {
  //    (map: Map[String, String], k: String) => {
  //      map
  //    }
  //  }

}
