package com.zeotap.data.io.source.utils

import com.zeotap.data.io.common.constructs.SupportedFeatureOps.featuresCompiler
import com.zeotap.data.io.common.types.SupportedFeaturesHelper.SupportedFeaturesF
import com.zeotap.data.io.source.spark.interpreters.SparkInterpreters._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object SparkLoaderUtils {

  def buildLoader(readerProperties: Seq[SupportedFeaturesF[DataFrameReader]],
                  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]],
                  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]])(implicit spark: SparkSession): DataFrame = {
    val reader = featuresCompiler(readerProperties).foldMap[SparkReader](readerInterpreter).run(spark.read)
    val dataFrame = featuresCompiler(readerToDataFrameProperties).foldMap[SparkReader](readerToDataFrameInterpreter).run(reader)

    if (dataFrameProperties.nonEmpty) {
      featuresCompiler(dataFrameProperties).foldMap[SparkDataFrame](dataFrameInterpreter).run(dataFrame).value._1
    } else dataFrame
  }

}
