package com.zeotap.source.loader.spark.constructs

import com.zeotap.cloudstorageutils.CloudStorePathMetaGenerator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{input_file_name, udf, unix_timestamp}
import org.apache.spark.sql.types.StringType

object CreationTimestampOps {

  def addRawTimestampColumnFromInputFilePath(dataFrame: DataFrame)(implicit cloudStorePathMetaGenerator: CloudStorePathMetaGenerator = new CloudStorePathMetaGenerator()): DataFrame = {
    val pathTsMap = cloudStorePathMetaGenerator.partFileRawTsMapGenerator(getPathsArray(dataFrame))

    val addRawTimestampColumn: UserDefinedFunction = udf((x: String) => {
      pathTsMap.get(x)
    })

    dataFrame.withColumn("CREATED_TS_raw", unix_timestamp(addRawTimestampColumn(input_file_name()), "yyyy-MM-dd HH:mm").cast(StringType))
  }

  /**
   * Takes the entire input file path (path + file name) and selects only till the last "/"
   *
   * @Input gs://file1/2020/05/31/payclick.csv
   * @Output gs://file1/2020/05/31/
   * @return The path till the last "/". The part files' path would be trimmed
   */
  def getPathsArray(dataFrame: DataFrame): Array[String] = {
    val addInputPathColumn: UserDefinedFunction = udf((inputFileName: String) => {
      inputFileName.reverse.substring(inputFileName.reverse.indexOf('/')).reverse
    })

    dataFrame.withColumn("inputPathColumn", addInputPathColumn(input_file_name())).select("inputPathColumn").distinct().collect.map(row => row.getString(0))
  }

}
