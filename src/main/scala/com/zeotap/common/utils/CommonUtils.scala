package com.zeotap.common.utils

object CommonUtils {

  def handleException[A](blockOfCode: => A): Either[String, A] = try {
    Right(blockOfCode)
  } catch {
    case e: Exception => Left(e.getLocalizedMessage)
  }

}
