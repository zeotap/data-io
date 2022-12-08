package com.zeotap.data.io.source.spark.constructs

import com.zeotap.data.io.source.spark.constructs.LookBackOps.getAllPossiblePaths
import com.zeotap.data.io.source.utils.DataPickupUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

object DataFrameReaderOps {

  implicit class DataFrameReaderExt(dataFrameReader: DataFrameReader) {

    /**
     * Look-back operation performed to obtain all available paths before the given date and within the lookBackWindow.
     * All the obtained paths are loaded as a single `DataFrame` and returned.
     * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
     * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
     */
    def lookBack(pathTemplate: String, parameters: Map[String, String], lookBackWindow: Integer): DataFrame = {
      val possiblePaths = getAllPossiblePaths(pathTemplate, parameters, lookBackWindow)
      val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)
      val pathsToPick = LookBackOps.getPathsToPick(possiblePaths, fileSystem)
      safeReadMultiPath(pathsToPick)
    }

    /**
     * The latest paths (sub-folders for a given pathTemplate) with respect to the given date are obtained and loaded into a `DataFrame`.
     * If relativeToCurrentDate is set to true, the obtained paths will be of a date prior to the provided date and
     * if it is set to false, the latest paths available in the fileSystem will be returned.
     * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
     * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
     */
    // Another example for pathTemplate: gs://bucket/*/yr=${YR}/mon=${MON}/dt=${DT}, this would return the latest paths for
    // gs://bucket/path1/yr=${YR}/mon=${MON}/dt=${DT}, gs://bucket/path2/yr=${YR}/mon=${MON}/dt=${DT}, etc.
    def latestPaths(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): DataFrame = {
      val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)
      val paths = LatestPathOps.getAllLatestPaths(new Path(pathTemplate), fileSystem, parameters, relativeToCurrentDate)
      safeReadMultiPath(paths)
    }

    private def safeReadMultiPath(paths: List[String]): DataFrame = {
      val initialDf = dataFrameReader.load(paths.head)
      paths.tail.foldLeft(initialDf)((accDf, path) => {
        val currDf = dataFrameReader.load(path)
        val prevColumns = accDf.columns.toSet
        val currColumns = currDf.columns.toSet
        val allColumns = prevColumns ++ currColumns

        def selectColumns(currColumns: Set[String], allColumns: Set[String]) = {
          allColumns.toList.map {
            case x if currColumns.contains(x) => col(x)
            case x => lit(null).as(x)
          }
        }

        accDf.select(selectColumns(prevColumns, allColumns) : _*).union(currDf.select(selectColumns(currColumns, allColumns) : _*))
      })
    }

  }

}
