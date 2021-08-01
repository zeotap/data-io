package com.zeotap.source.loader.spark.loaders

object SparkLoader {

  def text: TextSparkLoader = TextSparkLoader()

  def csv: CSVSparkLoader = CSVSparkLoader()

  def json: JSONSparkLoader = JSONSparkLoader()

  def avro: AvroSparkLoader = AvroSparkLoader()

  def parquet: ParquetSparkLoader = ParquetSparkLoader()

  def orc: ORCSparkLoader = ORCSparkLoader()

  def jdbc: JDBCSparkLoader = JDBCSparkLoader()

  def bigquery: BigQuerySparkLoader = BigQuerySparkLoader()
}
