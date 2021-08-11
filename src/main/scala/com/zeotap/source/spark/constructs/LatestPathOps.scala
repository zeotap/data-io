package com.zeotap.source.spark.constructs

import com.zeotap.source.utils.DataPickupUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}

object LatestPathOps {

  val PATH_DELIMITER = "/"
  val WILDCARD_CHARACTER = "*"
  val EMPTY_STRING = ""
  val STRATEGY_TYPE = "latestPath"
  val RELATIVE_TO_CURRENT_DATE_CHECK_KEY = "relativeToCurrentDate"
  val DATE_INFIX_KEY = "dateInfix"
  val DATE_PARAMS = List("HR", "MIN", "YR", "MON", "DT")
  val SPARK_SUCCESS_TAG = "_SUCCESS"

  def getPathsForPattern(fs: FileSystem, pathPattern: String): Array[Path] = {
    // determine upto one depth if path is generated as a result of spark computation by checking for presence of success tags
    // if no such paths found, fallback to simple path listing for given input pattern
    val sparkPaths = fs.globStatus(new Path("%s/%s".format(pathPattern, SPARK_SUCCESS_TAG)))
      .map(p => new Path(p.getPath.toString.stripSuffix(SPARK_SUCCESS_TAG)))
    if(sparkPaths.isEmpty)
      fs.globStatus(new Path(pathPattern)).map(_.getPath)
    else
      sparkPaths
  }

  def getAllLatestPaths(pathTemplate: Path, fs: FileSystem, parameters: Map[String, String], relativeToCurrentDate: Boolean): List[String] = {
    val wildCardedPathTemplate = DataPickupUtils.populatePathTemplateWithParameters(pathTemplate.toString,
      List("DT", "MON", "YR", "MIN", "HR").map((_, WILDCARD_CHARACTER)).toMap
    )
    val pathList = getPathsForPattern(fs, wildCardedPathTemplate)
    pathList.groupBy(path => getNonDateFields(path, pathTemplate)).mapValues(matchingPathList => {
      val referenceDatePath = DataPickupUtils.populatePathTemplateWithParameters(getDateFields(pathTemplate, pathTemplate),
        parameters)
      if (relativeToCurrentDate) {
        val prevPaths = matchingPathList.filter(p => getDateFields(p, pathTemplate) < referenceDatePath).map(_.toString)
        if (prevPaths.nonEmpty) prevPaths.max else EMPTY_STRING
      }
      else {
        if (matchingPathList nonEmpty)
          matchingPathList.map(_.toString).max
        else EMPTY_STRING
      }
    }).values.filter(StringUtils.isNotEmpty).toList
  }

  def getDateFields(path: Path, pathTemplate: Path): String = {
    val pathTemplateFields = pathTemplate.toString.split(PATH_DELIMITER)
    val indexedFields = for (i <- pathTemplateFields.indices)
      yield (pathTemplateFields(i), i)
    val dateFieldIndices = indexedFields
      .filter(field => DATE_PARAMS exists field._1.contains)
      .map(_._2)

    dateFieldIndices.map(path.toString.split(PATH_DELIMITER)(_))
      .mkString(PATH_DELIMITER)
  }

  def getNonDateFields(path: Path, pathTemplate: Path): String = {
    val pathTemplateFields = pathTemplate.toString.split(PATH_DELIMITER)
    val indexedFields = for (i <- pathTemplateFields.indices)
      yield (pathTemplateFields(i), i)

    val nonDateFieldIndices = indexedFields
      .filter(field => !(DATE_PARAMS exists field._1.contains)).map(_._2)

    nonDateFieldIndices.map(path.toString.split(PATH_DELIMITER)(_))
      .mkString(PATH_DELIMITER)
  }

}
