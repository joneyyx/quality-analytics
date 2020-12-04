package mck.qb.library
import org.apache.spark.sql.functions._
import scala.util.{Failure, Success, Try}
object MeasurementUnits extends Serializable {
  // TODO use enum instead of string units this will avoid alias or duplicate units
  object FinalUnits extends Enumeration {
    type FinalUnits = Value
    val KRPM,RPM, KW, N_M, DEG_C, KPA,
    PERCENT, PPM ,SECOND,KPA_A,KPA_G,BAR_A,BAR,CUMECS,
    VOLT,KMPH,KM,KMPL,LITRE,REVSPKM,N_MPSECOND= Value
    def Str(value: Value): String =value.toString
  }
  import FinalUnits._


  val targetUnitMap = Map(
    "_Advertised_Engine_Power_RPM"->Str(RPM),
    "_Advertised_Engine_Power"->Str(KW),
    "_Peak_Torque"->Str(N_M),
    "_Peak_Torque_RPM"->Str(RPM),
    "_DC_Mon_High_Cutoff_RPM"->Str(RPM),
    "_DC_Mon_Low_Cutoff_RPM"->Str(RPM),
    "ADVERTISED_POWER_AT_RPM"->Str(KW),
    "PEAK_TORQUE_AT_RPM"->Str(N_M),
    "PEAK_TORQUE_RPM"->Str(RPM),
    "ADVERTISED_POWER_RPM" -> Str(RPM),
    "LOW_CUTOFF_RPM"->Str(RPM),
    "HIGH_CUTOFF_RPM"->Str(RPM)
    ,
    //insite fault snapshot primary job
    "AFTERTREATMENT_DIESEL_PARTICULATE_FILTER_OUTLET_PRESSURE"->Str(KPA_G),
    "AFTERTREATMENT_FUEL_PRESSURE"->Str(KPA_A),
    "BAROMETRIC_AIR_PRESSURE"->Str(KPA),
    "OEM_AMBIENT_AIR_TEMPERATURE"->Str(DEG_C),
    "AFTERTREATMENT_DIESEL_OXIDATION_CATALYST_INTAKE_TEMPERATURE"->Str(DEG_C),
    "AFTERTREATMENT_DIESEL_PARTICULATE_FILTER_DIFFERENTIAL_PRESSURE"->Str(KPA),
    "AFTERTREATMENT_DIESEL_PARTICULATE_FILTER_OUTLET_TEMPERATURE"->Str(DEG_C),
    "PERCENT_ACCELERATOR_PEDAL_OR_LEVER"->Str(PERCENT),
    "TURBOCHARGER_COMPRESSOR_OUTLET_AIR_TEMPERATURE_CALCULATED"->Str(DEG_C),
    "ENGINE_COOLANT_TEMPERATURE"->Str(DEG_C),
    "CRANKCASE_PRESSURE"->Str(KPA_G),
    "AFTERTREATMENT_DIESEL_EXHAUST_FLUID_PRESSURE"->Str(KPA),
    "ENGINE_HOURS"->Str(SECOND),
    "ECM_TIME_KEY_ON_TIME"->Str(SECOND),
    "EGR_ORIFICE_PRESSURE"->Str(KPA_A),
    "EGR_TEMPERATURE"->Str(DEG_C),
    "EGR_VALVE_POSITION_MEASURED_PERCENT_OPEN"->Str(PERCENT),
    "EGR_VALVE_POSITION_COMMANDED"->Str(PERCENT),
    "EGR_DIFFERENTIAL_PRESSURE"->Str(KPA),
    "ENGINE_SPEED"->Str(RPM),
    "EXHAUST_GAS_PRESSURE"->Str(KPA),
    "EXHAUST_GAS_TEMPERATURE_CALCULATED"->Str(DEG_C),
    "FUEL_RAIL_PRESSURE_MEASURED"->Str(BAR_A),
    "FUEL_RAIL_PRESSURE_COMMANDED"->Str(BAR),
    "PERCENT_LOAD"->Str(PERCENT),
    "INTAKE_MANIFOLD_PRESSURE"->Str(KPA_G),
    "INTAKE_MANIFOLD_AIR_TEMPERATURE"->Str(DEG_C),
    "ENGINE_OIL_PRESSURE"->Str(KPA_G),
    "ENGINE_OIL_TEMPERATURE"->Str(DEG_C),
    "PERCENT_REMOTE_ACCELERATOR_PEDAL_OR_LEVER"->Str(PERCENT),
    "VEHICLE_SPEED"->Str(KMPH),
    "AFTERTREATMENT_SCR_INTAKE_TEMPERATURE"->Str(DEG_C),
    "AFTERTREATMENT_SCR_OUTLET_TEMPERATURE"->Str(DEG_C),
    "ENGINE_DISTANCE"->Str(KM),
    "TURBOCHARGER_SPEED"->Str(KRPM),
    "TURBOCHARGER_ACTUATOR_POSITION_MEASURED_PERCENT_CLOSED"->Str(PERCENT),
    "ECM_TIME"->Str(SECOND),
    "TURBOCHARGER_COMPRESSOR_INLET_AIR_TEMPERATURE"->Str(DEG_C),
    "ACCELERATOR_PEDAL_OR_LEVER_POSITION_SENSOR_SUPPLY_VOLTAGE"->Str(VOLT),
    "AFTERTREATMENT_DIESEL_EXHAUST_FLUID_QUALITY"->Str(PERCENT),
    "AFTERTREATMENT_INTAKE_NOX_CORRECTED"->Str(PPM),
    "AFTERTREATMENT_OUTLET_NOX_CORRECTED"->Str(PPM),
    "BATTERY_VOLTAGE"->Str(VOLT),
    "EXHAUST_VOLUMETRIC_FLOWRATE"->Str(CUMECS),
    "INTAKE_AIR_THROTTLE_POSITION"->Str(PERCENT),
    "INTAKE_AIR_THROTTLE_POSITION_COMMANDED"->Str(PERCENT),
    "SENSOR_SUPPLY_1"->Str(VOLT),
    "SENSOR_SUPPLY_2"->Str(VOLT),
    "SENSOR_SUPPLY_3"->Str(VOLT),
    "SENSOR_SUPPLY_4"->Str(VOLT),
    "SENSOR_SUPPLY_5"->Str(VOLT),
    "SENSOR_SUPPLY_6"->Str(VOLT),
    "TURBOCHARGER_ACTUATOR_POSITION_COMMANDED_PERCENT_CLOSED"->Str(PERCENT),
    "TURBOCHARGER_COMPRESSOR_INLET_AIR_TEMPERATURE_CALCULATED"->Str(DEG_C),
    // TRIP INFORMATION PRIMARY
    "ECM_TIME_KEY_ON_TIME"->Str(SECOND),
    "AVERAGE_ENGINE_SPEED"->Str(RPM),
    "ENGINE_BRAKE_DISTANCE"->Str(KM),
    "ENGINE_BRAKE_TIME"->Str(SECOND),
    "ENGINE_RUN_TIME"->Str(SECOND),
    "IDLE_FUEL_USED"->Str(LITRE),
    "AVERAGE_ENGINE_LOAD"->Str(PERCENT),
    "ENGINE_DISTANCE"->Str(KM),
    "FUEL_USED"->Str(LITRE),
    "FULL_LOAD_OPERATION_TIME"->Str(SECOND),
    "IDLE_TIME"->Str(SECOND),
    "AVERAGE_VEHICLE_SPEED"->Str(KMPH),
    "AFTERTREATMENT_DIESEL_EXHAUST_FLUID_USED"->Str(LITRE),
    "AVERAGE_FUEL_ECONOMY"->Str(KMPL),
    "COAST_DISTANCE"->Str(KM),
    "COAST_FUEL_USED"->Str(LITRE),
    "COAST_TIME"->Str(SECOND),
    "CRUISE_CONTROL_DISTANCE"->Str(KM),
    "CRUISE_CONTROL_FUEL_USED"->Str(LITRE),
    "CRUISE_CONTROL_TIME"->Str(SECOND),
    "DIESEL_EXHAUST_FLUID_TO_FUEL_CONSUMPTION_RATIO"->Str(PERCENT),
    "DRIVE_AVERAGE_ENGINE_SPEED"->Str(RPM),
    "DRIVE_AVERAGE_FUEL_ECONOMY"->Str(KMPL),
    "DRIVE_AVERAGE_LOAD"->Str(PERCENT),
    "DRIVE_AVERAGE_POWER"->Str(KW),
    "DRIVE_FUEL_USED"->Str(LITRE),
    "DRIVE_TIME"->Str(SECOND),
    "ECM_DISTANCE"->Str(KM),
    "ENGINE_PROTECTION_TIME_DERATED"->Str(SECOND),
    "FAN_ON_TIME"->Str(SECOND),
    "FAN_TIME_DUE_TO_AIR_CONDITIONING_PRESSURE_SWITCH"->Str(SECOND),
    "FAN_TIME_DUE_TO_ENGINE_CONDITIONS"->Str(SECOND),
    "FAN_TIME_DUE_TO_FAN_CONTROL_SWITCH"->Str(SECOND),
    "FAN_TIME_WITH_VEHICLE_SPEED"->Str(SECOND),
    "FAN_TIME_WITHOUT_VEHICLE_SPEED"->Str(SECOND),
    "FUEL_CONSUMED_FOR_AFTERTREATMENT_INJECTION"->Str(LITRE),
    "GEAR_DOWN_DISTANCE"->Str(KM),
    "GEAR_DOWN_FUEL_USED"->Str(LITRE),
    "GEAR_DOWN_TIME"->Str(SECOND),
    "MAXIMUM_ACCELERATOR_VEHICLE_SPEED"->Str(KMPH),
    "MAXIMUM_ACCELERATOR_VEHICLE_SPEED_DISTANCE"->Str(KM),
    "MAXIMUM_ACCELERATOR_VEHICLE_SPEED_FUEL_USED"->Str(LITRE),
    "MAXIMUM_ACCELERATOR_VEHICLE_SPEED_TIME"->Str(SECOND),
    "MAXIMUM_DIESEL_PARTICULATE_FILTER_DIFFERENTIAL_PRESSURE"->Str(KPA),
    "MAXIMUM_ENGINE_SPEED"->Str(RPM),
    "MAXIMUM_VEHICLE_SPEED"->Str(KMPH),
    "OUT_OF_GEAR_COAST_TIME"->Str(SECOND),
    "PREDICTIVE_CRUISE_CONTROL_DISTANCE"->Str(KM),
    "PREDICTIVE_CRUISE_CONTROL_FUEL_USED"->Str(LITRE),
    "PREDICTIVE_CRUISE_CONTROL_TIME"->Str(SECOND),
    "SERVICE_BRAKE_DISTANCE"->Str(KM),
    "SERVICE_BRAKE_TIME"->Str(SECOND),
    "SMART_TORQUE_2_LEVEL_1_ECONOMY_TIME"->Str(SECOND),
    "SMART_TORQUE_2_LEVEL_2_ECONOMY_TIME"->Str(SECOND),
    "TOP_GEAR_DISTANCE"->Str(KM),
    "TOP_GEAR_FUEL_USED"->Str(LITRE),
    "TOP_GEAR_TIME"->Str(SECOND),
    "TRIP_DRIVE_DISTANCE"->Str(KM),
    "VEHICLE_OVERSPEED_1_DISTANCE"->Str(KM),
    "VEHICLE_OVERSPEED_1_FUEL_USED"->Str(LITRE),
    "VEHICLE_OVERSPEED_1_TIME"->Str(SECOND),
    "VEHICLE_OVERSPEED_2_DISTANCE"->Str(KM),
    "VEHICLE_OVERSPEED_2_FUEL_USED"->Str(LITRE),
    "VEHICLE_OVERSPEED_2_TIME"->Str(SECOND),
    //Features and parameter
    "CRUISE_CONTROL_LOWER_DROOP"->Str(KMPH),
  "CRUISE_CONTROL_UPPER_DROOP"->Str(KMPH),
  "GEAR_DOWN_MAXIMUM_VEHICLE_SPEED_HEAVY_ENGINE_LOAD"->Str(KMPH),
  "GEAR_DOWN_MAXIMUM_VEHICLE_SPEED_LIGHT_ENGINE_LOAD"->Str(KMPH),
  "LOW_IDLE_SPEED"->Str(RPM),
  "MAXIMUM_ACCELERATOR_VEHICLE_SPEED"->Str(KMPH),
  "MAXIMUM_CRUISE_CONTROL_SPEED"->Str(KMPH),
  "ROAD_SPEED_GOVERNOR_LOWER_DROOP"->Str(KMPH),
  "ROAD_SPEED_GOVERNOR_UPPER_DROOP"->Str(KMPH),
  "RPM_BREAKPOINT"->Str(RPM),
  "TIME_BEFORE_SHUTDOWN"->Str(SECOND),
  "TIRE_SIZE"->Str(REVSPKM),
  "TORQUE_RAMP_RATE"->Str(N_MPSECOND),
    "VEHICLE_OVERSPEED_2_TIME"->Str(SECOND),
    //Engine abuse history
    "TIMECOLUMN"->Str(SECOND),
  "ENGINE COOLANT TEMPERATURE"->Str(DEG_C),
  "INTAKE MANIFOLD AIR TEMPERATURE"->Str(DEG_C),
  "ENGINE OIL PRESSURE"->Str(KPA_G),
  "ENGINE OIL TEMPERATURE"->Str(DEG_C),
  "CRANKCASE PRESSURE"->Str(KPA_G),
  "ENGINE SPEED"->Str(RPM)
  )

  // all untis need to be specified in the targetunitmap
  def getFinalUnitorElseNone = udf {
    (paramName:String, currUnit:String) =>
      targetUnitMap.getOrElse(paramName,"NONE")
  }

  def getFinalUnit = udf {
    (paramName:String, currUnit:String) =>

      if(targetUnitMap.contains(paramName))
        targetUnitMap.get(paramName).get
      else
        currUnit
  }

  def   convert= udf{
    (sourceUnit:String, paramName:String, actualValue:String) =>
     val finalresult=  (sourceUnit, paramName, actualValue) match {
        case (null|"NULL"|"null"|"",null|"NULL"|"null"|"",null|"NULL"|"null"|"")=>null
        case(x ,null|"NULL"|"null"|"" , y)=>null
        case(x ,y , null|"NULL"|"null"|"")=>null
        case( null|"NULL"|"null"|"" ,x , y)=>actualValue
        case _=>{
          if(targetUnitMap.contains(paramName)) {
            val tgt_Unit = targetUnitMap(paramName)
            var value: String = actualValue.toString.replaceAll(",", ".")

            val tryResult=Try((sourceUnit, tgt_Unit) match {
              case ("KPA"|"KPA_A"|"KPA_G", "KPA_G"|"KPA"|"KPA_A") => value
              case ("KPA_A"|"KPA"|"KPA_G", "INHG") => Pressure.kpaToInhg(value)
              case ("KPA"|"KPA_G"|"KPA_A", "PSI") => Pressure.kpaToPsi(value)
              case ("KPA"|"KPA_G"|"KPA_A", "BAR" | "BAR_A") => Pressure.kpaTobar(value)
              case ("INHG"|"INH", "KPA_G"|"KPA_A"|"KPA") => Pressure.inhgToKpa(value)
              case ("INHG"|"INH", "PSI") => Pressure.inhgToKpa(value)
              case ("MMHG","KPA_G"|"KPA_A"|"KPA") => Pressure.mmhgToKpa(value)
              case ("PSI"|"PSIA", "KPA_G"|"KPA_A"|"KPA") => Pressure.psiToKpa(value)
              case ("BAR" | "BAR_A", "BAR" | "BAR_A") => value
              case ("BAR"|"BAR_A", "KPA_G"|"KPA_A"|"KPA") => Pressure.barToKpa(value)
              case ("BAR" | "BAR_A", "PSI") => Pressure.barToPsi(value)
              case ("PSI","BAR" | "BAR_A") => Pressure.psiTobar(value)

              case ("INH2O", "KPA_G"|"KPA_A"|"KPA") => Pressure.inh2oToKpa(value)
              case ("INH2O", "PSI") => Pressure.inh2oToPsi(value)

              case("P"|"PERCENT"|"PERC"|"PERCE"|"PE"|"PERCENT","PERCENT") => value

              case ("KM" |"K"|"KM/HR"|"KMPH", "KM" |"KMPH") => value
              case ("MI" | "MPH", "KM" | "KM/HR"|"KMPH") => Speed.miToKm(value)
              case ("KM" | "KM/HR", "MI" | "MPH") => Speed.kmToMi(value)
              case ("M","KM") => Speed.mToKm(value)
              case ("REVS/MI","REVS/KM"|"REVSPKM") => Speed.revspmiTorevspkm(value)
              case ("KM/L","KMPL")=>value

              case ("GAL", "L"|"LITRE") => Volume.galToL(value)
              case ("L", "GAL") => Volume.lToGal(value)
              case ("IGAL", "L"|"LITRE") => Volume.iGalToL(value)
              case ("IGAL", "GAL") => Volume.iGalToGal(value)
              case("L","LITRE")=>value

              case ("MI/GAL"|"MPG", "KM/L"|"KMPL") => Distance.miPerGalToKmPerL(value)
              case ("MI/IGAL","KM/L"|"KMPL")=>Distance.miPerIGalToKmPerL(value)
              case ("KM/L", "MI/GAL") => Distance.kmPerLToMiPerGal(value)
              case ("L/100 KM","KMPL")=> Distance.ltsper100kmtoKmpl(value)

              case ("°C" | "DEG_C"|"C", "°C" | "DEG_C") => value
              case ("°F" | "DEG_F" |"F", "°C" | "DEG_C") => Temperature.fToC(value)
              case ("°C" | "DEG_C"|"C", "°F" | "DEG_F") => Temperature.cToF(value)

              case ("RP"|"RPM", "RPM") => value
              case ("KPRM", "RPM") => Torque.KprmToRpm(value)
              case ("RPM"|"RP"|"R", "KRPM") => Torque.rpmToKRpm(value)

              case ("FT3/S", "M3/S"|"CUMECS") => Distance.cubFtPSecToCubMPSec(value)
              case("M3/S","CUMECS")=>value

              case ("SECOND" | "S", "HOURS") => Time.secToHr(value)
              case ("HHHHHH:MM:SS" | "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
                    "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M", "HOURS") => Time.timeToHr(value)
              case ("HHHHHH:MM:SS" | "HH:MM:" | "HH:MM:S" | "HH:" | "HHHHH"| "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
                    "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M"|"HRS"|"HHH"|"HHHHHH:MM:S", "SECOND"|"S") => Time.timeToSec(value)

              case ("HP"|"H", "KW") => Power.hpToKw(value)
              case ("KW", "HP") => Power.kwToHp(value)
              case ("PS", "KW") => Power.psTokw(value)

              case ("V"|"VOLT"|"VOLTAGE","VOLT")=>value
              case ("S","SECOND")=>value
              case ("FT*LB","N_M") => Torque.ftPLbToNpM(value)
              case ("FT*LBF/S","N_MPSECOND") => Torque.ftPLbToNpM(value)
              case ("KGF*M","N_M") => Torque.kgFMToNpM(value)
              case ("N*M/S" ,"N_MPSECOND" ) => value
              case (x,y) if x.equalsIgnoreCase(y) => value
              case _ => null
            }
            )
            val result:String=tryResult match {
              case Success(v) =>
                if(v==null){
                  println("conversion formula not present",sourceUnit,tgt_Unit,paramName,actualValue)
                }
                v
              case Failure(e) =>
                println("conversion error Info from the exception: " +  e.getMessage,sourceUnit,tgt_Unit,paramName,actualValue)
                null
            }
            result
          }else
          {
            actualValue
          }
        }
      }
      finalresult
  }



  object Pressure extends Serializable{
    def inh2oToKpa(value: String): String =(value.toDouble * 0.2490).toString
    def mmhgToKpa(value: String): String= (value.toDouble *  0.1333).toString
    def barToKpag(value: String):String = ((value.toDouble * 100)-101.3).toString
    def psiToKpag(value: String):String = ((value.toDouble * 6.894)-101.3).toString
    def kpaToKpag(value: String): String =(value.toDouble - 101.3).toString
    def inhgToKpag(value: String): String =((value.toDouble * 3.386) - 101.3).toString
    def inhgToKpa(value:String):String = (value.toDouble * 3.386).toString
    def inhgToPsi(value:String):String = (value.toDouble * 0.49).toString
    def kpaToInhg(value:String):String = (value.toDouble * 0.2952).toString
    def barToKpa(value:String):String = (value.toDouble * 100).toString
    def kpaTobar(value:String):String = (value.toDouble / 100).toString
    def barToPsi(value:String):String = (value.toDouble * 14.50).toString
    def psiTobar(value:String):String = (value.toDouble / 14.50).toString
    def inh2oToPsi(value:String):String = (value.toDouble * 0.036).toString
    def psiToKpa(value:String):String = (value.toDouble * 6.894).toString
    def kpaToPsi(value:String):String = (value.toDouble * 0.145).toString
  }

  object Speed extends Serializable{
    def miToKm(value:String):String = (value.toDouble * 1.609).toString
    def mToKm(value:String):String= (value.toDouble*0.001).toString
    def kmToMi(value:String):String = (value.toDouble * 0.6213).toString
    def revspmiTorevspkm(value:String) :String = (value.toDouble / 1.609).toString
  }

  object Volume extends Serializable{
    def galToL(value:String):String = (value.toDouble * 3.785).toString
    def lToGal(value:String):String = (value.toFloat * 0.2641).toString
    def iGalToL(value:String):String = (value.toDouble * 1.2009 * 3.785).toString
    def iGalToGal(value:String):String = (value.toFloat * 1.2009).toString

  }

  object Temperature extends Serializable{
    def fToC(value:String):String = ((value.toDouble - 32) / 1.8).toString
    def cToF(value:String):String = ((value.toDouble * 9) / 5 + 32).toString

  }

  object Distance extends Serializable{
    def ltsper100kmtoKmpl(value: String): String = (100/value.toDouble).toString
    def miPerGalToKmPerL(value:String):String = (value.toDouble * 0.425).toString
    def kmPerLToMiPerGal(value:String):String = (value.toDouble / 2.35214583).toString
    def cubFtPSecToCubMPSec(value:String):String = (value.toDouble * 0.0283168).toString
    def miPerIGalToKmPerL(value:String):String = (value.toDouble*0.354).toString
  }

  object Time extends Serializable {
    def secToHr(value: String): String = (value.toDouble / 3600).toString
    def timeToHr(value: String): String = {
      val hhmmss = "\\d+:\\d+:\\d+".r
      val hhmm = "\\d+:\\d+:?".r
      val hh="\\d+.?\\d*:?".r
      value match {
        case hhmmss() => {
          val Array(hour, min, sec) = value.split(":")
          hour.toDouble + (min.toDouble/60.0) + (sec.toDouble/3600).toString
        }
        case hhmm() => {
          val Array (hour, min) = value.split(":")
          (hour.toDouble + (min.toDouble/60.0)).toString

        }
        case hh() => {
          val Array(hour) = value.split(":")
          hour.toDouble.toString
        }
        case _ => null
      }
    }
    //"HHHHHH:MM:SS" | "HH:MM:SS" | "HHHHHH:MM" | "HHHHHH" | "HHHH" |
    //                  "HHHHHH:MM:" | "HH" | "HHHHHH:" | "HHHHHH:M"
    def timeToSec(value: String): String = {
      val hhmmss = "\\d+:\\d+:\\d+".r
      val hhmm = "\\d+:\\d+:?".r
      val hh="\\d+.?\\d*:?".r
      value match {
        case hhmmss() => {
          val Array(hour, min, sec) = value.split(":")
          ((hour.toDouble*3600 )+ (min.toDouble*60.0) + sec.toDouble).toString
        }
        case hhmm() => {
          val Array (hour, min) = value.split(":")
          ((hour.toDouble*3600) + (min.toDouble*60.0)).toString

        }
        case hh() => {
          val Array(hour) = value.split(":")
          (hour.toDouble*3600).toString
        }
        case _ => null
      }
    }
  }
  object Power extends Serializable{
    def kwToHp(value:String):String = (value.toDouble / 1.3597).toString
    def hpToKw(value:String):String = (value.toDouble * 0.7457).toString
    def psTokw(value:String):String = (value.toDouble * 0.7355).toString
  }

  object Torque extends Serializable{
    def rpmToKRpm(value: String): String = (value.toDouble / 1000).toString
    def ftPLbToNpM(value:String):String = (value.toDouble * 1.36).toString
    def kgFMToNpM(value:String):String =  (value.toDouble * 9.807).toString
    def KprmToRpm(value:String):String = (value.toDouble * 1000).toString
  }
}
