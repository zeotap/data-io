package com.zeotap.source.loader.spark.constructs

import com.zeotap.source.loader.spark.constructs.LookBackOps.getAllPossiblePaths
import com.zeotap.source.loader.utils.DataPickupUtils
import org.apache.spark.sql.{DataFrame, DataFrameReader}

object DataFrameReaderOps {

  implicit class DataFrameReaderOps(dataFrameReader: DataFrameReader) {

    def lookBack(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): DataFrame = {
      val possiblePaths = getAllPossiblePaths(pathTemplate, parameters, lookBackWindow)
      val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)
      val pathsToPick = LookBackOps.getPathsToPick(possiblePaths, fileSystem)
      readMultiPath(pathsToPick)
    }

    def latestPath(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): DataFrame = {
      val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)
      val pathsToPick = LatestPathOps.getPathsToPick(pathTemplate, fileSystem, parameters, relativeToCurrentDate)
      readMultiPath(pathsToPick)
    }

    def readMultiPath(paths: List[String]): DataFrame = {
      val schema = dataFrameReader.load(paths.head).schema
      dataFrameReader.schema(schema).load(paths: _*)
    }

  }

}