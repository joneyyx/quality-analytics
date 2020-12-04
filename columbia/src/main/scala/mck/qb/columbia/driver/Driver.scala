package mck.qb.columbia.driver

import mck.qb.columbia.config.AppConfig
import mck.qb.columbia.config.AppConfig.AppConfigException
import mck.qb.library.Util
import mck.qb.library.sample.SuperJob
import org.apache.log4j.{Level, Logger}

object Driver extends Serializable {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(Driver.getClass)
    ApplicationProperties.parserArgs( args )

    val hmJobs: java.util.HashMap[String, Array[String]] = new java.util.HashMap[String, Array[String]]()
    ApplicationProperties.DATASOURCE.foreach( source => {
      if (ApplicationProperties.PIPELINE.length == 0) {
        var arrLayers: Array[String] = Array[String]()

        if (ApplicationProperties.LAYER.contains("PRI")) arrLayers :+= "PRI"
        if (ApplicationProperties.LAYER.contains("FET")) arrLayers :+= "FET"
        if (ApplicationProperties.LAYER.contains("MIP")) arrLayers :+= "MIP"
        if (ApplicationProperties.LAYER.contains("MOP")) arrLayers :+= "MOP"
        if (ApplicationProperties.LAYER.contains("RPT")) arrLayers :+= "RPT"

        arrLayers.foreach(layer => this._parseJobConfig(logger, hmJobs, "%s.%s".format(layer, source)))
      }
      else {
        ApplicationProperties.PIPELINE.foreach(pipeline => {
          var arrLayers: Array[String] = Array[String]()

          if (ApplicationProperties.LAYER.contains("PRI")) arrLayers :+= "PRI"
          if (ApplicationProperties.LAYER.contains("FET")) arrLayers :+= "FET"
          if (ApplicationProperties.LAYER.contains("MIP")) arrLayers :+= "MIP"
          if (ApplicationProperties.LAYER.contains("MOP")) arrLayers :+= "MOP"
          if (ApplicationProperties.LAYER.contains("RPT")) arrLayers :+= "RPT"

          arrLayers.foreach(layer => this._parseJobConfig(logger, hmJobs, "%s.%s.%s".format(layer, source, pipeline), pipeline))
        })
      }
    })

    if (ApplicationProperties.PIPELINE.length == 0) {
      import scala.collection.JavaConversions._
      hmJobs.foreach(jobs => {
        logger.log(Level.INFO, "Pipeline - (%s) :: Jobs Order - (%s)".format(jobs._1, jobs._2.mkString(",")))
        jobs._2.foreach(sJobName => {
          this._jobSubmit(logger, jobs._1, sJobName)
        })
      })
    }
    else {
      ApplicationProperties.PIPELINE.foreach(pipeline => {
        try {
          hmJobs.get("%s.%s".format(pipeline, "CN")).foreach(sJobName => {
            this._jobSubmit(logger, pipeline, sJobName)
          })
        }
        catch {             // Added NullPointerException
          case e: NullPointerException =>
            logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(pipeline, e.getMessage))
            throw e
        }
      })
    }

    Util.getSpark().close()
  }

  private def _parseJobConfig(logger: Logger, hmJobs: java.util.HashMap[String, Array[String]], sConfigKey: String, sPipeline: String = null): Unit = try {
    val objJobConfig = AppConfig.getMyConfig(sConfigKey)
    val itrJobConfig = objJobConfig.entrySet().iterator()

    while (itrJobConfig.hasNext) {
      val sKey = itrJobConfig.next().getKey
      if (sKey.endsWith("CN")) {
        val sPipelineKey: String = if (sPipeline == null) sKey else "%s.%s".format(sPipeline, sKey)
        var arrJobs: Array[String] = hmJobs.getOrDefault(sPipelineKey, Array[String]())
        arrJobs :+= objJobConfig.getString(sKey)
        hmJobs.put(sPipelineKey, arrJobs)
      }
    }
  }
  catch {
    case e: AppConfigException => logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(sPipeline,e.getMessage))
    case e: NullPointerException =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(sPipeline,e.getMessage))
      throw e
  }

  private def _jobSubmit(logger: Logger, sPipeline: String, sJobName: String): Unit = try {
    logger.log(Level.INFO, "Process Initiated for Job (%s) - (%s)".format(sPipeline, sJobName))

    Class.forName("%s$".format(sJobName)).getField("MODULE$").get(classOf[SuperJob]).
      asInstanceOf[SuperJob].main(null) //HV158 - added superjob QI1312

    logger.log(Level.INFO, "Process Completed for Job (%s) - (%s)".format(sPipeline, sJobName))
  }
  catch {
    case e: Exception =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(sJobName,e.getMessage))
      throw e
    case e: Throwable =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(sJobName,e.getMessage))
      throw e
    case e: NullPointerException =>
      logger.log(Level.ERROR, " (%s) job failed..Error message is - (%s)".format(sJobName,e.getMessage))
      throw e
  }

}
