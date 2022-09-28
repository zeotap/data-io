package com.zeotap.data.io.common.test.helpers

import java.io.{File, PrintWriter}

object CommonUtils {

  def writeToFile(path: String, data: String): Unit = {
    val fileObject = new File(path)
    val printWriter = new PrintWriter(fileObject)

    printWriter.write(data)
    printWriter.close()
  }

}
