package com.zeotap.data.io.common.types

sealed trait DataType

case object String extends DataType

case object Boolean extends DataType

case object Byte extends DataType

case object Short extends DataType

case object Int extends DataType

case object Long extends DataType

case object Float extends DataType

case object Double extends DataType

case object Decimal extends DataType

case object Date extends DataType

case object Timestamp extends DataType

object DataType {

  def value(dataType: DataType): String = {
    dataType match {
      case String => "string"
      case Boolean => "boolean"
      case Byte => "byte"
      case Short => "short"
      case Int => "int"
      case Long => "long"
      case Float => "float"
      case Double => "double"
      case Decimal => "decimal"
      case Date => "date"
      case Timestamp => "timestamp"
    }
  }

  def valueOf(dataType: String): DataType = {
    dataType match {
      case "string" => String
      case "boolean" => Boolean
      case "byte" => Byte
      case "short" => Short
      case "int" => Int
      case "long" => Long
      case "float" => Float
      case "double" => Double
      case "decimal" => Decimal
      case "date" => Date
      case "timestamp" => Timestamp
    }
  }

}
