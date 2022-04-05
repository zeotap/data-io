package com.zeotap.data.io.source.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.data.io.common.types.Overwrite
import com.zeotap.data.io.sink.spark.writer.ParquetSparkWriter
import com.zeotap.data.io.source.spark.loader.AvroSparkLoader
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Assert.assertEquals
import org.scalatest.FunSuite

import java.io.File

class SplitOpsTest extends FunSuite with DataFrameSuiteBase {

  val rawInputPath: String = "src/test/resources/custom-input-format/yr=2022/mon=03/dt=26"
  val intermediatePath: String = "src/test/resources/custom-input-format/yr=2022/mon=03/dt=26_intermediate"


  override def beforeAll(): Unit = {
    super.beforeAll()

    val testSchema = List(
      StructField("Common_DataPartnerID", IntegerType, nullable = true),
      StructField("DeviceId", StringType, nullable = true),
      StructField("Demographic_Country", StringType, nullable = true),
      StructField("Common_TS", StringType, nullable = true)
    )

    val sampleDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "1", "India", "1504679559"),
        Row(1, "2", "India", "1504679359"),
        Row(1, "3", "Spain", "1504679459"),
        Row(1, "4", "India", "1504679659"),
        Row(2, "1", "India", "1504679759"),
        Row(2, "2", "India", "1504679359"),
        Row(2, "3", "Spain", "1504679959"),
        Row(2, "4", "India", "1504679859")
      )),
      StructType(testSchema)
    )

    sampleDataFrame.write.format("avro").save(rawInputPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(rawInputPath))
    FileUtils.forceDelete(new File(intermediatePath))
  }


  test("default split test") {

    val numberOfPartitions = 3
    val intermediatePath: String = "src/test/resources/custom-input-format/yr=2022/mon=03/dt=26_intermediate"
    val prioritiseIntermediatePath = false

    val actualDf = AvroSparkLoader().load(rawInputPath).buildUnsafe(spark)

    val rawInputDf = AvroSparkLoader()
      .load(rawInputPath)
      .split(numberOfPartitions, intermediatePath, prioritiseIntermediatePath)
      .buildUnsafe(spark)

    assertEquals(rawInputDf.rdd.getNumPartitions, 3)
    assertDataFrameEquality(actualDf, rawInputDf, "DeviceId")
  }

  test("Split when intermediate data is already available") {

    val numberOfPartitions = 5
    val prioritiseIntermediatePathList = List(true, false)

    val actualDf = AvroSparkLoader().load(rawInputPath).buildUnsafe(spark)

    //Existing intermediate data (num of partitions = 3)
    ParquetSparkWriter().addSaveMode(Overwrite).save(intermediatePath).buildUnsafe(actualDf.repartition(3))

    prioritiseIntermediatePathList.foreach(priority => {
      val rawInputDf = AvroSparkLoader().load(rawInputPath)
        .split(numberOfPartitions, intermediatePath, priority)
        .buildUnsafe(spark)

      if (priority) {
        assertEquals(rawInputDf.rdd.getNumPartitions, 3)
        assertDataFrameEquality(actualDf, rawInputDf, "DeviceId")
      } else {
        assertEquals(rawInputDf.rdd.getNumPartitions, 5)
        assertDataFrameEquality(actualDf, rawInputDf, "DeviceId")
      }
    })
  }


}
