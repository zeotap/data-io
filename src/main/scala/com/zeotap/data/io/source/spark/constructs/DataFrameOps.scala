package com.zeotap.data.io.source.spark.constructs

import com.zeotap.data.io.common.types.{DataType, OptionalColumn, Overwrite}
import com.zeotap.data.io.common.utils.{CloudStorePathMetaGenerator, logger}
import com.zeotap.data.io.sink.spark.writer.ParquetSparkWriter
import com.zeotap.data.io.source.spark.loader.ParquetSparkLoader
import com.zeotap.data.io.source.utils.DataPickupUtils.{getFileSystem, pathExists}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.util.{Failure, Success, Try}

object DataFrameOps {

  implicit class DataFrameExt(dataFrame: DataFrame) {

    /**
     * Only if a provided column does not exist in the DataFrame, it will be added with the provided defaultValue.
     * This option can be used when a certain column is not provided by a DP everyday but is required in the further operations
     * @param columns is a list of OptionalColumn(columnName: String, defaultValue: String, dataType: DataType)
     * Supported dataTypes = {STRING, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, DATE, TIMESTAMP}
     */
    def addOptionalColumns(columns: List[OptionalColumn]): DataFrame = {
      val dataFrameColumns = dataFrame.columns
      columns.foldLeft(dataFrame)((accDf, optionalColumn) =>
        if (!dataFrameColumns.contains(optionalColumn.columnName))
          accDf.withColumn(optionalColumn.columnName, lit(optionalColumn.defaultValue).cast(DataType.value(optionalColumn.dataType)))
        else accDf
      )
    }

    /**
     * Adds a column based on the provided operation
     * @param operation needs to be one of `addColumn`, `renameColumn`
     * case `addColumn` => outputTsColumn calculated from the flat files' create TS
     * case `renameColumn` => existing timestamp column value used for outputTsColumn
     */
    def appendRawTsToDataFrame(operation: String, inputColumn: Option[String], outputColumn: String)(implicit cloudStorePathMetaGenerator: CloudStorePathMetaGenerator = new CloudStorePathMetaGenerator()): DataFrame = {
      operation match {
        case "addColumn" =>
          if (dataFrame.columns.contains(outputColumn)) throw new IllegalStateException(s"$outputColumn column already exists")
          else dataFrame.addRawTimestampColumnFromInputFilePath(outputColumn)
        case "renameColumn" =>
          if (dataFrame.columns.contains(inputColumn.get)) dataFrame.withColumn(outputColumn, col(inputColumn.get))
          else throw new NoSuchElementException(s"there is no $inputColumn column in the provided input")
        case _ => throw new IllegalArgumentException("Valid operation is not provided")
      }
    }

    /**
     * Adds outputTsColumn calculated from the flat files' create TS to the input dataFrame
     */
    def addRawTimestampColumnFromInputFilePath(outputColumn: String)(implicit cloudStorePathMetaGenerator: CloudStorePathMetaGenerator = new CloudStorePathMetaGenerator()): DataFrame = {
      val pathTsMap = cloudStorePathMetaGenerator.partFileRawTsMapGenerator(dataFrame.getPathsArray)

      val addRawTimestampColumn: UserDefinedFunction = udf((x: String) => {
        pathTsMap.get(x)
      })

      dataFrame.withColumn(outputColumn, unix_timestamp(addRawTimestampColumn(input_file_name()), "yyyy-MM-dd HH:mm").cast(StringType))
    }

    /**
     * Takes the entire input file path (path + file name) and selects only till the last "/"
     *
     * @Input gs://file1/2020/05/31/payclick.csv
     * @Output gs://file1/2020/05/31/
     * @return The path till the last "/". The part files' path would be trimmed
     */
    def getPathsArray: Array[String] = {
      val addInputPathColumn: UserDefinedFunction = udf((inputFileName: String) => {
        inputFileName.reverse.substring(inputFileName.reverse.indexOf('/')).reverse
      })

      dataFrame.withColumn("inputPathColumn", addInputPathColumn(input_file_name())).select("inputPathColumn").distinct().collect.map(row => row.getString(0))
    }

    /**
     * Partitions given dataframe into number of partitions provided and writes the
     * partitioned dataframe to the intermediate path provided (partitioning happens only if the intermediate data is not already present),
     * so that parallel processing can take place.
     *
     * @param numberOfPartitions         is the number of partitions we want in the dataframe.
     * @param intermediatePath           is the intermediate path in which the partitioned data frame is written.
     * @param prioritiseIntermediatePath is boolean value which denotes whether the intermediate path (i.e already partitioned path)
     *                                   needs to prioritised or should we force repartition. If it is true then data at intermediate path
     *                                   will be returned(if non empty).
     * @return Returns Dataframe with specified number of partitions.
     */

    def distributedLoad(numberOfPartitions: Option[Int], intermediatePath: String, prioritiseIntermediatePath: Option[Boolean]): DataFrame = {
      val intermediateDataPriority = prioritiseIntermediatePath getOrElse true
      val successFilePath = intermediatePath + "/_SUCCESS"
      val spark = dataFrame.sparkSession
      if (!intermediateDataPriority || !pathExists(successFilePath, getFileSystem(successFilePath))) {
        defaultSplit(numberOfPartitions, intermediatePath)
      }
      else {
        Try(ParquetSparkLoader().load(intermediatePath).buildUnsafe(spark)) match {
          case Success(data) => data
          case Failure(e) =>
            logger.log.info(s"Failed to load data from $intermediatePath! \nError: ${e.getMessage}  \nProceeding with default split strategy!")
            defaultSplit(numberOfPartitions, intermediatePath)
        }
      }
    }

    /*
    * Default Splitting strategy, takes Raw Input Dataframe, re-partitions it, writes the partitioned dataframe
    * to intermediate path, loads the dataframe present at intermediate path and returns it for further processing.
     */
    def defaultSplit(numberOfPartitions: Option[Int], intermediatePath: String): DataFrame = {
      val spark = dataFrame.sparkSession
      val numberOfPartitionsRequired = numberOfPartitions getOrElse 200
      val partitionedData = dataFrame.repartition(numberOfPartitionsRequired)
      ParquetSparkWriter().addSaveMode(Overwrite).save(intermediatePath).buildUnsafe(partitionedData)
      ParquetSparkLoader().load(intermediatePath).buildUnsafe(spark)
    }
  }

}
