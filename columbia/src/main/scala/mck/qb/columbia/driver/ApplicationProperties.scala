package mck.qb.columbia.driver

import java.util

object ApplicationProperties extends Serializable {

  class ParseArgsException( s : String ) extends Exception( s ) {}

  var DATASOURCE, LAYER, PIPELINE: Array[String] = Array[String]()

  var ALIAS, TARGET_JCEKS, HOSTNAME, USERNAME = ""
  val PACKAGE_NAME: String = "%s".format(this.getClass.getPackage.getName.replaceFirst("driver", "connector"))

  val hmErrors: util.Map[String, String] = new util.HashMap[String, String]()
  hmErrors.put("E001", "ERROR :: E001 - Argument missing - %s=?")
  hmErrors.put("E002", "ERROR :: E002 - Dependency missing - %s")
  hmErrors.put("E003", "ERROR :: E003 - Argument is empty required option - %s=?")

  def getError(sCode : String, sBuild: String): String = hmErrors.get(sCode).format(sBuild)

  def parserArgs( args : Array[ String ] ): Unit = {

    if (args.length == 0) {
      throw new ParseArgsException( this.getError("E003", "datasource") )
    }

    for ( i <- args.indices ) {
      val argsSplit = args( i ).split( "=" )

      val prefArg = argsSplit( 0 ).toLowerCase()

      val posfArgStr = argsSplit( 1 )
      val posfArg = argsSplit( 1 ).split(",").map(_.toUpperCase)

      prefArg match {
        case "datasource" => DATASOURCE = posfArg
        case "layer" => LAYER = posfArg
        case "pipeline" => PIPELINE = posfArg
        case "target_jceks" => TARGET_JCEKS = posfArgStr
        case "alias" => ALIAS = posfArgStr
        case "hostname" => HOSTNAME = posfArgStr
        case "username" => USERNAME = posfArgStr

        case _ =>
      }
    }

    // STOP APPLICATION IF MANDATORY PARAMETERS ARE NOT PRESENT
    if (DATASOURCE.length == 0)  throw new ParseArgsException( this.getError("E001", "datasource") )
    if (LAYER.length == 0) LAYER = Array[String]("PRI", "FET", "MIP", "MOP", "RPT")
  }
}
