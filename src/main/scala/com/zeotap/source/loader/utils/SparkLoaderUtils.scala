package com.zeotap.source.loader.utils

import com.zeotap.source.loader.spark.constructs.SparkReaderOps.featuresCompiler
import com.zeotap.source.loader.spark.interpreters.SparkInterpreters.{SparkDataFrame, SparkReader, dataFrameInterpreter, readerInterpreter, readerToDataFrameInterpreter}
import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object SparkLoaderUtils {

  def buildLoader(readerProperties: Seq[DataSourceLoader[DataFrameReader]],
                  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]],
                  dataFrameProperties: Seq[DataSourceLoader[DataFrame]])(implicit spark: SparkSession): DataFrame = {
    val props = featuresCompiler(readerProperties)
    val reader = props.foldMap[SparkReader](readerInterpreter).run(spark.read)
    val dataFrame = featuresCompiler(readerToDataFrameProperties).foldMap[SparkReader](readerToDataFrameInterpreter).run(reader)

    if (dataFrameProperties.nonEmpty) {
      featuresCompiler(dataFrameProperties).foldMap[SparkDataFrame](dataFrameInterpreter).run(dataFrame).value._1
    } else dataFrame
  }

}
