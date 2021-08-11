package com.zeotap.source.spark.constructs

import com.zeotap.cloudstorageutils.CloudStorePathMetaGenerator
import com.zeotap.common.types.{DataType, OptionalColumn}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

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
  }

}
