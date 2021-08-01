package com.zeotap.source.loader.spark.constructs

import cats.data.Reader
import com.zeotap.source.loader.spark.interpreters.SparkInterpreters.SparkReader
import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import org.apache.spark.sql.DataFrame

object SparkReaderOps {

  def featuresCompiler[A](features: Seq[DataSourceLoader[A]]): DataSourceLoader[A] =
    features.reduce((a, b) => for {
      _ <- a
      v2 <- b
    } yield v2)

  def readMultiPath(paths: List[String]): SparkReader[DataFrame] = Reader { dataFrameReader =>
    val schema = dataFrameReader.load(paths.head).schema
    dataFrameReader.schema(schema).load(paths: _*)
  }

}
