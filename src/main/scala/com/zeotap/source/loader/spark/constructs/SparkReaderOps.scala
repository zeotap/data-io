package com.zeotap.source.loader.spark.constructs

import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader

object SparkReaderOps {

  def featuresCompiler[A](features: Seq[DataSourceLoader[A]]): DataSourceLoader[A] =
    features.reduce((a, b) => for {
      _ <- a
      v2 <- b
    } yield v2)

}
