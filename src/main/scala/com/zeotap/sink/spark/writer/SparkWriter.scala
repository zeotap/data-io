package com.zeotap.sink.spark.writer

object SparkWriter {

  def text: TextSparkWriter = TextSparkWriter()

  def csv: CSVSparkWriter = CSVSparkWriter()

  def json: JSONSparkWriter = JSONSparkWriter()

  def avro: AvroSparkWriter = AvroSparkWriter()

  def parquet: ParquetSparkWriter = ParquetSparkWriter()

  def orc: ORCSparkWriter = ORCSparkWriter()

  def jdbc: JDBCSparkWriter = JDBCSparkWriter()

}
