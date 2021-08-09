package com.zeotap.source.spark.constructs

import com.zeotap.cloudstorageutils.CloudStorePathMetaGenerator
import com.zeotap.common.types.{DataType, OptionalColumn}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object DataFrameOps {

  implicit class DataFrameExt(dataFrame: DataFrame) {

    def addOptionalColumns(columns: List[OptionalColumn]): DataFrame = {
      val dataFrameColumns = dataFrame.columns
      columns.foldLeft(dataFrame)((accDf, optionalColumn) =>
        if (!dataFrameColumns.contains(optionalColumn.columnName))
          accDf.withColumn(optionalColumn.columnName, lit(optionalColumn.defaultValue).cast(DataType.value(optionalColumn.dataType)))
        else accDf
      )
    }

    def appendRawTsToDataFrame(inputType: String)(implicit cloudStorePathMetaGenerator: CloudStorePathMetaGenerator = new CloudStorePathMetaGenerator()): DataFrame = {
      inputType match {
        case "raw" =>
          if (dataFrame.columns.contains("CREATED_TS_raw")) throw new IllegalStateException("CREATED_TS_raw column already exists")
          else dataFrame.addRawTimestampColumnFromInputFilePath
        case "preprocess" =>
          if (dataFrame.columns.contains("CREATED_TS_raw")) dataFrame
          else throw new NoSuchElementException("there is no CREATED_TS_raw column in the transform input")
        case "tube" =>
          if (dataFrame.columns.contains("timestamp")) dataFrame.withColumn("CREATED_TS_raw", col("timestamp"))
          else throw new NoSuchElementException("there is no timestamp column in the transform input")
        case _ => throw new IllegalArgumentException("Valid Raw Input Path Type is not provided when requiredDataSourceBucketTS is set true")
      }
    }

    def addRawTimestampColumnFromInputFilePath(implicit cloudStorePathMetaGenerator: CloudStorePathMetaGenerator = new CloudStorePathMetaGenerator()): DataFrame = {
      val pathTsMap = cloudStorePathMetaGenerator.partFileRawTsMapGenerator(dataFrame.getPathsArray)

      val addRawTimestampColumn: UserDefinedFunction = udf((x: String) => {
        pathTsMap.get(x)
      })

      dataFrame.withColumn("CREATED_TS_raw", unix_timestamp(addRawTimestampColumn(input_file_name()), "yyyy-MM-dd HH:mm").cast(StringType))
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
