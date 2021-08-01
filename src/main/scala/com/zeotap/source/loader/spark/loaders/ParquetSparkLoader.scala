package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{PARQUET, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class ParquetSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(PARQUET)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  def mergeSchema: ParquetSparkLoader =
    ParquetSparkLoader(readerProperties :+ SupportedFeaturesF.mergeSchema, readerToDataFrameProperties, dataFrameProperties)
}
