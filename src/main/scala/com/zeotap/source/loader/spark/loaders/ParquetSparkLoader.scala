package com.zeotap.source.loader.spark.loaders

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{PARQUET, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader}

case class ParquetSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(PARQUET)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  /**
   * If the sub-directories in a given path contain datasets with different schemas,
   * this option will read the data into a single DataFrame with all the columns
   */
  def mergeSchema: ParquetSparkLoader =
    ParquetSparkLoader(readerProperties :+ SupportedFeaturesF.mergeSchema, readerToDataFrameProperties, dataFrameProperties)
}
