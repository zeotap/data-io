package com.zeotap.data.io.source.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File


class BloomFilterOpsTest extends FunSuite with DataFrameSuiteBase {

  val inputAvroPath : String = "src/test/resources/custom-input-format/avro"
  val bloomBasePath : String = "src/test/resources/custom-bloom/bloom"

  val testSchema = List(
    StructField("Id", IntegerType, true),
    StructField("DeviceId", StringType, true),
    StructField("Demographic_Country", StringType, true),
    StructField("Common_TS", StringType, true)
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sampleInputBloomDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(12345,"1","India","1504679559"),
        Row(13456,"2","India","1504679359"),
        Row(14567,"3","Spain","1504679459"),
        Row(15678,"4","India","1504679659")
      )),
      StructType(testSchema)
    )

    val sampleTestDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(12345,"1","India","1504679559"),
        Row(13456,"2","India","1504679359"),
        Row(14567,"3","Spain","1504679459"),
        Row(15678,"5","India","1504679659"),
        Row(16789,"5","India","1504679659")
      )),
      StructType(testSchema)
    )

    import com.zeotap.data.io.sink.spark.constructs.DataFrameOps._
    sampleInputBloomDataFrame.writeBloomFilter(List("Id", "DeviceId"), bloomBasePath, 4, 0.01)(spark)
    sampleTestDataFrame.write.format("avro").save(inputAvroPath)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputAvroPath))
    FileUtils.forceDelete(new File(bloomBasePath))
  }

  test("Bloom filter on single column") {
    val inputDataFrame = spark.read.format("avro").load(inputAvroPath)
    val dataframe = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(16789,"5","India","1504679659")
      )),
      StructType(testSchema)
    )

    import com.zeotap.data.io.source.spark.constructs.DataFrameOps._
    val actualDataFrame = inputDataFrame.filterByBloom(List("Id"), bloomBasePath)(spark)

    assertDataFrameEquals(dataframe, actualDataFrame)
  }

  test("Bloom filter on multiple columns") {
    val inputDataFrame = spark.read.format("avro").load(inputAvroPath)

    val dataframe = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(15678,"5","India","1504679659"),
        Row(16789,"5","India","1504679659")
      )),
      StructType(testSchema)
    )

    import com.zeotap.data.io.source.spark.constructs.DataFrameOps._
    val actualDataFrame = inputDataFrame.filterByBloom(List("Id", "DeviceId"), bloomBasePath)(spark)

    assertDataFrameEquals(dataframe, actualDataFrame)
  }
}
