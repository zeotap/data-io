package com.zeotap.source.spark.constructs

import com.zeotap.source.spark.constructs.LookBackOps.getAllPossiblePaths
import com.zeotap.source.utils.DataPickupUtils
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
     * The latest path with respect to the given date is obtained and loaded into a `DataFrame`.
     * If relativeToCurrentDate is set to true, the obtained path will be of a date prior to the provided date and
     * if it is set to false, the latest path available in the fileSystem will be returned.
     * @param parameters should contain values for YR, MON, DT. HR and MIN are optional.
     * @param pathTemplate Example: gs://bucket/path/yr=${YR}/mon=${MON}/dt=${DT}
     */
    def latestPath(pathTemplate: String, parameters: Map[String, String], relativeToCurrentDate: Boolean): DataFrame = {
      val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)
      val path = LatestPathOps.getLatestPath(new Path(pathTemplate), fileSystem, parameters, relativeToCurrentDate)
      dataFrameReader.load(path)
    }

    private def safeReadMultiPath(paths: List[String]): DataFrame = {
      val initialDf = dataFrameReader.load(paths.head)
      paths.foldLeft(initialDf)((accDf, path) => {
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
