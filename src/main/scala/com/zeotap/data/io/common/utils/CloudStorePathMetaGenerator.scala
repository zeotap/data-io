package com.zeotap.data.io.common.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable


class CloudStorePathMetaGenerator extends Serializable {

  /**
   * @param inputPathList Ex: val inputPathList = List("gs://path1/", "gs://path2/", "gs://path3/")
   *
   *                      Takes the inputPathList and perform the ls operation on all the paths and stores the part file and its corresponding timestamp in a map
   *                      Ex: Map("gs://path1/part.txt.gz"-> "2021-05-14 03:45","gs://path2/part1.txt.gz"-> "2021-05-14 03:45")
   * @return Map(PartFile -> Timestamp)
   */
  def partFileRawTsMapGenerator(inputPathList: Array[String]): Map[String, String] = {
    inputPathList.map(new Path(_)).flatMap(path => {
      val fileSystem = path.getFileSystem(new Configuration)
      val filesIterator = fileSystem.listFiles(path, true)
      val pathsMap = new mutable.HashMap[String, String]()
      while (filesIterator.hasNext) {
        val filePath = filesIterator.next().getPath
        val modificationDate = new Date(fileSystem.getFileStatus(filePath).getModificationTime)
        pathsMap.put(filePath.toUri.toString, new SimpleDateFormat("yyyy-MM-dd HH:mm").format(modificationDate))
      }
      pathsMap
    }).toMap
  }
}
