package com.zeotap.source.loader.spark.constructs

import com.zeotap.cloudstorageutils.CloudStorePathMetaGenerator
import com.zeotap.source.loader.spark.constructs.CreationTimestampOps.addRawTimestampColumnFromInputFilePath
import com.zeotap.source.loader.types.{DataType, OptionalColumn}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object DataFrameOps {

  implicit class DataFrameOps(dataFrame: DataFrame) {

    def optionalColumns(columns: List[OptionalColumn]): DataFrame = {
      val dataFrameColumns = dataFrame.columns
      columns.foldLeft(dataFrame)((accDf, optionalColumn) =>
        if (!dataFrameColumns.contains(optionalColumn.columnName))
          accDf.withColumn(optionalColumn.columnName, lit(optionalColumn.defaultValue).cast(DataType.value(optionalColumn.dataType)))
        else accDf
      )
    }

    def appendRawTsToDataFrame(inputType: String)(implicit cloudStorePathMetaGenerator: CloudStorePathMetaGenerator = new CloudStorePathMetaGenerator()): DataFrame = {
      inputType match {
        case "raw" => addRawTimestampColumnFromInputFilePath(dataFrame)
        case "preprocess" =>
          if (dataFrame.columns.contains("CREATED_TS_raw")) dataFrame
          else throw new NoSuchElementException("there is no CREATED_TS_raw column in the transform input")
        case "tube" =>
          if (dataFrame.columns.contains("timestamp")) dataFrame.withColumn("CREATED_TS_raw", col("timestamp"))
          else throw new NoSuchElementException("there is no timestamp column in the transform input")
        case _ => throw new IllegalArgumentException("Valid Raw Input Path Type is not provided when requiredDataSourceBucketTS is set true")
      }
    }
  }

}
