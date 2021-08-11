package com.zeotap.common.types

sealed trait DataType

case object STRING extends DataType

case object BOOLEAN extends DataType

case object BYTE extends DataType

case object SHORT extends DataType

case object INT extends DataType

case object LONG extends DataType

case object FLOAT extends DataType

case object DOUBLE extends DataType

case object DECIMAL extends DataType

case object DATE extends DataType

case object TIMESTAMP extends DataType

object DataType {

  def value(dataType: DataType): String = {
    dataType match {
      case STRING => "string"
      case BOOLEAN => "boolean"
      case BYTE => "byte"
      case SHORT => "short"
      case INT => "int"
      case LONG => "long"
      case FLOAT => "float"
      case DOUBLE => "double"
      case DECIMAL => "decimal"
      case DATE => "date"
      case TIMESTAMP => "timestamp"
    }
  }

}
