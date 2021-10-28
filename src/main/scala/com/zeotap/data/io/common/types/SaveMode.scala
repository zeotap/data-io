package com.zeotap.data.io.common.types

sealed trait SaveMode

case object Append extends SaveMode

case object ErrorIfExists extends SaveMode

case object Ignore extends SaveMode

case object Overwrite extends SaveMode

object SaveMode {

  def value(mode: SaveMode): String = {
    mode match {
      case Append => "append"
      case ErrorIfExists => "errorifexists"
      case Ignore => "ignore"
      case Overwrite => "overwrite"
    }
  }
}
