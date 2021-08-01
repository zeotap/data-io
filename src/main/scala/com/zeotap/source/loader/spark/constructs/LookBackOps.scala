package com.zeotap.source.loader.spark.constructs

import com.zeotap.source.loader.spark.interpreters.SparkInterpreters.SparkReader
import com.zeotap.source.loader.utils.DataPickupUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

object LookBackOps {

  def lookBackProgram[A](pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): SparkReader[DataFrame] = {
    val possiblePaths = getAllPossiblePaths(pathTemplate, parameters, lookBackWindow)
    val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)
    val pathsToPick = getPathsToPick(possiblePaths, fileSystem)
    for {
      dataFrame <- SparkReaderOps.readMultiPath(pathsToPick)
    } yield dataFrame
  }

  def getLocalDateTimeFromStandardParameters(parameters: Map[String, String]): LocalDateTime = {
    val year = parameters("YR").toInt
    val month = parameters("MON").toInt
    val day = parameters("DT").toInt
    val hour: Int = if (parameters.contains("HR")) parameters("HR").toInt else 0
    val minute: Int = if (parameters.contains("MIN")) parameters("MIN").toInt else 0
    LocalDateTime.of(year, month, day, hour, minute)
  }

  def getStandardParametersFromLocalDateTime(localDateTime: LocalDateTime): Map[String, String] = {
    val year = f"${localDateTime.getYear}%04d"
    val month = f"${localDateTime.getMonthValue}%02d"
    val day = f"${localDateTime.getDayOfMonth}%02d"
    val hour = f"${localDateTime.getHour}%02d"
    val minute = f"${localDateTime.getMinute}%02d"
    Map("YR" -> year, "MON" -> month, "DT" -> day, "HR" -> hour, "MIN" -> minute)
  }

  def getAllPossiblePaths(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): List[String] = {
    val localDateTime = getLocalDateTimeFromStandardParameters(parameters)
    (0 to lookBackWindow)
      .toList
      .map(localDateTime.minusDays(_))
      .map(getStandardParametersFromLocalDateTime)
      .map(parameters ++ _)
      .map(DataPickupUtils.populatePathTemplateWithParameters(pathTemplate, _))
  }

  def getPathsToPick(possiblePaths: List[String], fileSystem: FileSystem): List[String] =
    possiblePaths.filter(DataPickupUtils.pathExists(_, fileSystem))

}
