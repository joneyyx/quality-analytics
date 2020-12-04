package mck.qb.library

import java.util.Properties

import mck.qb.columbia.constants.Constants
import mck.qb.columbia.constants.Constants.{HDFS_PATH}
import mck.qb.columbia.driver.ApplicationProperties
import mck.qb.library.Util.{formatCapitalizeNames, getSpark}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

/**
  * Centralized I/O - data loading and saving (and everything around that) functionality.
  */
object IO extends Serializable {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /** *
    * Runs a block of code with "spark.sql.shuffle.partitions" set to the specified value and restored
    * (or reset to default) afterwards.
    *
    * @param numPartitions spark.sql.shuffle.partitions value to use
    * @param f             block of code to run
    */
  def withShufflePartition(numPartitions: Long)(f: Unit): Unit =
    withSparkConfVal("spark.sql.shuffle.partitions", numPartitions)(f)

  /** *
    * Runs a block of code with "spark.sql.autoBroadcastJoinThreshold" set to the specified value and restored
    * (or reset to default) afterwards.
    * Default is 10485760 (10 MB)
    *
    * @param threshold     spark.sql.shuffle.autoBroadcastJoinThreshold value to use
    * @param f             block of code to run
    */
  def withAutoBroadcastJoinThreshold(threshold: Long)(f: Unit): Unit =
    withSparkConfVal("spark.sql.autoBroadcastJoinThreshold", threshold)(f)

  /**
    * Runs a block of code with the config value (e.g., "spark.sql.shuffle.autoBroadcastJoinThreshold") set to
    * the specified value and restored
    *
    * @param confKey key to set
    * @param confVal value to use
    * @param f       block of code to run
    */
  def withSparkConfVal(confKey: String, confVal: Long)(f: Unit): Unit = {
    withSparkConfVal(confKey, confVal.toString)(f)
  }

  /**
    * Runs a block of code with the config value (e.g., "spark.sql.shuffle.autoBroadcastJoinThreshold") set to
    * the specified value and restored
    *
    * @param confKey key to set
    * @param confVal value to use
    * @param f       block of code to run
    */
  def withSparkConfVal(confKey: String, confVal: String)(f: Unit): Unit = {
    logger.debug(s"Setting ${confKey} to ${confVal}")
    val spark = getSpark()
    val orig = spark.conf.getOption(confKey)
    spark.conf.set(confKey, confVal)
    try {
      f
    } finally {
      logger.debug(s"Resetting ${confKey} to ${orig}")
      orig match {
        case Some(origVal) => spark.conf.set(confKey, origVal)
        case None => spark.conf.unset(confKey)
      }
    }
  }

  /**
    * Mix-in this trait to get functionality for loading and saving data to/from ADLS, SQL Server, etc.
    */
  trait Transport {

    /**
      * Define to return config properties by key,
      * e.g.
      * override def cfg: String => String = getConfig().getString _
      *
      * @return
      */
    def cfg: String => String

    /**
      * Runs SQL query, returning resulting DataFrame.
      * WARNING: this call directly uses the SQL query expressed as a string, SQL injection is possible.
      *
      * @param sqlText       table name (specify in the following format "database.table")
      * @param numPartitions specify partition count if repartitioning is needed, omit if no repartitioning needed
      * @return resulting DataFrame
      */
    def load(sqlText: String, numPartitions: Option[Int] = None): DataFrame = {
      logger.debug(s"Executing SQL: ${sqlText} with repartitioning: ${numPartitions}")
      val df = formatCapitalizeNames(Util.getSpark().sql(sqlText))
      numPartitions.map(df.repartition(_)).getOrElse(df)
    }

    /**
      * Read specified columns of all rows from the specified table.
      *
      * @param numPartitions number of partitions or None if no repartitioning is needed
      * @param table         table name (specify in the following format "database.table")
      * @param col       columns to read
      * @param cols
      * @return results
      */
    def load(numPartitions: Option[Int])(table: String, col: String, cols: String*): DataFrame = {
      logger.debug(s"Reading: ${table}.(${col}, ${cols}) with repartitioning: ${numPartitions}")
      val df = formatCapitalizeNames(Util.getSpark().table(tableName = table).select(col, cols: _*))
      numPartitions.map(df.repartition(_)).getOrElse(df)
    }

    /**
      * Read specified columns of all rows from the specified table.
      *
      * @param table
      * @param col
      * @param cols
      * @return results
      */
    def load(table: String, col: String, cols: String*): DataFrame = {
      load(None)(table, col, cols: _*)
    }

    def loadCsv(storedAs:String, isHeader:String, table: String, col: String, cols: String*): DataFrame = {
      val tblinfo = table.split("\\.")
      if(storedAs.toLowerCase().equals("csv")){
        print(Util.getWarehousePath(cfg(HDFS_PATH),tblinfo(0),tblinfo(1)))
        Util.getSpark().read.format(storedAs).
          options(Map("header"->isHeader,"delimiter"->"|")).load(Util.getWarehousePath(cfg(HDFS_PATH),tblinfo(0),tblinfo(1))+"/*.csv")
      }else{
        Util.getSpark().read.format(storedAs).load(Util.getWarehousePath(cfg(HDFS_PATH),tblinfo(0),tblinfo(1)))
      }
    }

    /**
      * Read specified seq columns of all rows from the specified table.
      *
      * @param table      table name (specify in the following format "database.table")
      * @param seqCols    sequence of columns to read
      * @return results
      */
    def load(table: String, seqCols: Seq[String]): DataFrame = {
      formatCapitalizeNames(Util.getSpark().table(tableName = table).selectExpr(seqCols: _*))
    }

    /**
      * Read specified columns of all rows from the specified table, performing repartitioning by the specified key.
      *
      * @param numPartitions number of partitions after repartitioning
      * @param partitionExpr partition key
      * @param table         table name (specify in the following format "database.table")
      * @param col
      * @param cols          columns
      * @return results
      */
    def load(numPartitions: Int, partitionExpr: org.apache.spark.sql.Column*)(table: String, col: String, cols: String*): DataFrame = {
      val df = formatCapitalizeNames(Util.getSpark().table(tableName = table).select(col, cols: _*))
      df.repartition(numPartitions, partitionExpr: _*)
    }

    /**
      * Read all columns of all rows from the specified table, performing repartitioning by the specified key.
      *
      * @param numPartitions number of partitions after repartitioning
      * @param partitionExpr partition key
      * @param table         table name (specify in the following format "database.table")
      * @return results
      */
    def load(table: String, numPartitions: Int, partitionExpr: org.apache.spark.sql.Column*): DataFrame = {
      formatCapitalizeNames(Util.getSpark().table(tableName = table)).repartition(numPartitions, partitionExpr: _*)
    }

    def loadFile(path: String, layer: String, solution: String, file: String, compression: String = "snappy"): DataFrame = {
      formatCapitalizeNames(Util.getSpark().read.option("compression", compression).parquet(path + "/" + layer + "/" + solution + "/" + file))
    }

    /**
      * Repartition dataframe by the key.
      *
      * @param numPartitions resulting number of partitions
      * @param partitionExpr partition key
      * @param df            dataframe to repartition
      * @return results
      */
    def partitionBy(numPartitions: Int, partitionExpr: org.apache.spark.sql.Column*)(df: DataFrame): DataFrame = {
      df.repartition(numPartitions, partitionExpr: _*)
    }

    /**
      * Returns a DataFrameWriter
      *
      * @param df   data to save
      * @param mode "overwrite","append",etc.
      * @return a DataFrameWrite that can be used to save by the caller
      */
    private def dfWriter(df: DataFrame, mode: String = "overwrite"): DataFrameWriter[Row] =
      formatCapitalizeNames(df).write.mode(mode)

    /**
      * Saves data frame to target table.
      *
      * @param df     data to save
      * @param db     database name
      * @param tbl    table name
      * @param mode   write option mode, default = overwrite
      * @param format write format, default = parquet
      */
    def save(df: DataFrame, db: String, tbl: String, mode: String = "overwrite", format: String = "parquet"): Unit = {
      logger.debug(s"Saving $db.$tbl - Format ($format)")

      format match {
        case "csv" => dfWriter(df.repartition(1)).
          options(Map(Constants.PATH -> "%s_%s".format(Util.getWarehousePath(cfg(HDFS_PATH), db, tbl), format))).
          option("compression", "none").
          format(format).save()

        case _ => dfWriter(df).
          options(Map(Constants.PATH -> Util.getWarehousePath(cfg(HDFS_PATH), db, tbl))).
          format(format).
          saveAsTable(s"$db.$tbl")
      }
    }

    //HV158 - Deprecating saveRpt , action needs to be performed by DataTransformer
  }

}
