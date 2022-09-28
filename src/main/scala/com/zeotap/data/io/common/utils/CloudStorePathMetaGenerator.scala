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
    inputPathList.map(path => new Path(path)).flatMap(path => {
      val fileSystem = path.getFileSystem(new Configuration)
      val fileStatuses = fileSystem.globStatus(path)
      val pathsMap = new mutable.HashMap[String, String]()
      fileStatuses.foreach(fileStatus => {
        val filePath = fileStatus.getPath
        val filesIterator = fileSystem.listFiles(filePath, true)
        while (filesIterator.hasNext) {
          val subFilePath = filesIterator.next().getPath
          val modificationDate = new Date(fileSystem.getFileStatus(subFilePath).getModificationTime)
          pathsMap.put(subFilePath.toUri.toString, new SimpleDateFormat("yyyy-MM-dd HH:mm").format(modificationDate))
        }
      })
      pathsMap
    }).toMap
  }
}
