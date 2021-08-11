package com.zeotap.common.types

sealed trait SaveMode

case object APPEND extends SaveMode

case object ERROR_IF_EXISTS extends SaveMode

case object IGNORE extends SaveMode

case object OVERWRITE extends SaveMode

object SaveMode {

  def value(mode: SaveMode): String = {
    mode match {
      case APPEND => "append"
      case ERROR_IF_EXISTS => "errorifexists"
      case IGNORE => "ignore"
      case OVERWRITE => "overwrite"
    }
  }
}
