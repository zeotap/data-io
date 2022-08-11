package com.zeotap.data.io.source.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.data.io.common.types.Overwrite
import com.zeotap.data.io.sink.spark.writer.ParquetSparkWriter
import com.zeotap.data.io.source.spark.loader.AvroSparkLoader
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Assert.assertEquals
import org.scalatest.FunSuite

import java.io.File

class SplitOpsTest extends FunSuite with DataFrameSuiteBase {

  val rawInputPath: String = "src/test/resources/custom-input-format/yr=2022/mon=03/dt=26"
  val intermediatePath: String = "src/test/resources/custom-input-format/yr=2022/mon=03/dt=26_intermediate"

  override def conf: SparkConf = super.conf.set("spark.default.parallelism", "500")

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteQuietly(new File(rawInputPath))
    FileUtils.deleteQuietly(new File(intermediatePath))

    val testSchema = List(
      StructField("Common_DataPartnerID", IntegerType, nullable = true),
      StructField("DeviceId", StringType, nullable = true),
      StructField("Demographic_Country", StringType, nullable = true),
      StructField("Common_TS", StringType, nullable = true)
    )
    val rowList = List(
      Row(1, "1", "India", "1504679559"),
      Row(1, "2", "India", "1504679359"),
      Row(1, "3", "Spain", "1504679459"),
      Row(1, "4", "India", "1504679659"),
      Row(2, "1", "India", "1504679759"),
      Row(2, "2", "India", "1504679359"),
      Row(2, "3", "Spain", "1504679959"),
      Row(2, "4", "India", "1504679859")
    )

    val dataFrameRows = (0 to 1000).toList.foldLeft(Seq[Row]())((rowSeq, ind) => {
      rowSeq :+ rowList(ind % 8)
    })

    val sampleDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(dataFrameRows), StructType(testSchema))
    sampleDataFrame.coalesce(1).write.format("avro").save(rawInputPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(new File(rawInputPath))
    FileUtils.deleteQuietly(new File(intermediatePath))
  }

  test("Default splitting strategy with intermediatePath parameter only") {
    val expectedDf = AvroSparkLoader().load(rawInputPath).buildUnsafe(spark)
    val actualDf = AvroSparkLoader().load(rawInputPath).distributedLoad(intermediatePath).buildUnsafe(spark)

    assertDataFrameEquality(actualDf, expectedDf, sortColumn = "DeviceId")
    assertEquals(200, actualDf.rdd.getNumPartitions)
  }


  test("Test for default splitting when intermediate path should not be prioritised") {

    val numberOfPartitions = 3
    val prioritiseIntermediatePath = false

    val actualDf = AvroSparkLoader().load(rawInputPath).buildUnsafe(spark)

    val rawInputDf = AvroSparkLoader()
      .load(rawInputPath)
      .distributedLoad(intermediatePath, numberOfPartitions, prioritiseIntermediatePath)
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

    /*
      * Aims to test the split strategy that is being followed for case when intermediate data is already present
      * Input number of partitions for intermediate data is 3
      * Case 1 => When we need to prioritise intermediate path, In this case output df should have 3 partitions irrespective of the fact that we've provided number of partitions as 5.
      * Case 2 => When we need not to prioritise intermediate path, In this case output df should have 5 partitions because we provided number of partitions as 5.
     */
    prioritiseIntermediatePathList.foreach(priority => {
      val rawInputDf = AvroSparkLoader().load(rawInputPath)
        .distributedLoad(intermediatePath, numberOfPartitions, priority)
        .buildUnsafe(spark)
      //here priority defines whether we need to prioritise the intermediate path or not.
      if (priority.equals(true)) {
        assertEquals(rawInputDf.rdd.getNumPartitions, 3)
        assertDataFrameEquality(actualDf, rawInputDf, "DeviceId")
      } else {
        assertEquals(rawInputDf.rdd.getNumPartitions, 5)
        assertDataFrameEquality(actualDf, rawInputDf, "DeviceId")
      }
    })
  }


}
