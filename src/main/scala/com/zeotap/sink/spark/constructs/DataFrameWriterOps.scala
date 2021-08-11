package com.zeotap.sink.spark.constructs

import org.apache.spark.sql.DataFrameWriter

object DataFrameWriterOps {

  implicit class DataFrameWriterExt(dataFrameWriter: DataFrameWriter[_]) {

    def partitionBy(columnNames: List[String]): DataFrameWriter[_] =
      if (columnNames.nonEmpty) dataFrameWriter.partitionBy(columnNames : _*) else dataFrameWriter

  }

}
