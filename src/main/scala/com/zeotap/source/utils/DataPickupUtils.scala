package com.zeotap.source.utils

import org.apache.commons.text.StringSubstitutor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConversions

object DataPickupUtils {

  def getFileSystem(pathTemplate: String): FileSystem =
    new Path(pathTemplate).getFileSystem(new Configuration)

  def pathExists(path: String, fs: FileSystem): Boolean =
    fs.exists(new Path(path))

  def populatePathTemplateWithParameters(pathTemplate: String, parameters: Map[String, String]): String =
    new StringSubstitutor(JavaConversions.mapAsJavaMap(parameters)).replace(pathTemplate)
}
