package com.zeotap.source.loader.spark.loaders

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.zeotap.source.loader.types.SupportedFeaturesF.DataSourceLoader
import com.zeotap.source.loader.types.{AVRO, SupportedFeaturesF}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

case class AvroSparkLoader(
  readerProperties: Seq[DataSourceLoader[DataFrameReader]] = Seq(SupportedFeaturesF.addFormat(AVRO)),
  readerToDataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq(),
  dataFrameProperties: Seq[DataSourceLoader[DataFrame]] = Seq()
) extends FSSparkLoader(readerProperties, readerToDataFrameProperties, dataFrameProperties) {

  def avroSchema(schema: String): AvroSparkLoader =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesF.avroSchema(schema), readerToDataFrameProperties, dataFrameProperties)

  def avroSchema(jsonSchema: JsonNode): AvroSparkLoader =
    AvroSparkLoader(readerProperties :+ SupportedFeaturesF.avroSchema(new ObjectMapper().writeValueAsString(jsonSchema)), readerToDataFrameProperties, dataFrameProperties)
}
