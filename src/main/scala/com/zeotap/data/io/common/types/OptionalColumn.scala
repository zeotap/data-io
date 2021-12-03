package com.zeotap.data.io.common.types

case class OptionalColumn(columnName: String, defaultValue: String, dataType: DataType) {

  def getConvertedValue: Any = DataType.convert(dataType, defaultValue)

  def getStringDataType: String = DataType.value(dataType)

}
