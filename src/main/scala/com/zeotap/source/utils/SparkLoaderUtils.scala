package com.zeotap.source.utils

import com.zeotap.common.constructs.SupportedFeatureOps.featuresCompiler
import com.zeotap.common.types.SupportedFeaturesF.SupportedFeaturesF
import com.zeotap.source.spark.interpreters.SparkInterpreters._
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object SparkLoaderUtils {

  def buildLoader(readerProperties: Seq[SupportedFeaturesF[DataFrameReader]],
                  readerToDataFrameProperties: Seq[SupportedFeaturesF[DataFrame]],
                  dataFrameProperties: Seq[SupportedFeaturesF[DataFrame]])(implicit spark: SparkSession): DataFrame = {
    val props = featuresCompiler(readerProperties)
    val reader = props.foldMap[SparkReader](readerInterpreter).run(spark.read)
    val dataFrame = featuresCompiler(readerToDataFrameProperties).foldMap[SparkReader](readerToDataFrameInterpreter).run(reader)

    if (dataFrameProperties.nonEmpty) {
      featuresCompiler(dataFrameProperties).foldMap[SparkDataFrame](dataFrameInterpreter).run(dataFrame).value._1
    } else dataFrame
  }

}
