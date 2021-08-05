package com.zeotap.common.types

sealed trait DataFormatType

case object TEXT extends DataFormatType

case object CSV extends DataFormatType

case object JSON extends DataFormatType

case object AVRO extends DataFormatType

case object PARQUET extends DataFormatType

case object ORC extends DataFormatType

case object JDBC extends DataFormatType

case object BIGQUERY extends DataFormatType

object DataFormatType {

  def value(format: DataFormatType): String = {
    format match {
      case TEXT => "text"
      case CSV => "csv"
      case JSON => "json"
      case AVRO => "avro"
      case PARQUET => "parquet"
      case ORC => "orc"
      case JDBC => "jdbc"
      case BIGQUERY => "bigquery"
    }
  }
}
