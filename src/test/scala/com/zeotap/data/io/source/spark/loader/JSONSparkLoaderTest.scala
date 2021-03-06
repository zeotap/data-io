package com.zeotap.data.io.source.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import java.io.File

class JSONSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

  val inputJsonPath1 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19"
  val inputJsonPath2 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18"

  override def beforeAll(): Unit = {
    super.beforeAll()

    val testSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val sampleDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(testSchema)
    )

    sampleDf.write.format("json").save(inputJsonPath1)

    val testSchema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true)
    )

    val sampleDf2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5"),
        Row(1,"Germany","1504679359","6"),
        Row(1,"Italy","1504679459","7"),
        Row(1,"Belgium","1504679659","8")
      )),
      StructType(testSchema2)
    )

    sampleDf2.write.option("multiLine", "true").format("json").save(inputJsonPath2)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputJsonPath1))
    FileUtils.forceDelete(new File(inputJsonPath2))
  }

  test("testForLoadPath") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", LongType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1L,"1","India","1504679559"),
        Row(1L,"2","India","1504679359"),
        Row(1L,"3","Spain","1504679459"),
        Row(1L,"4","India","1504679659")
      )),
      StructType(expectedSchema)
    )

    val df = JSONSparkLoader()
      .load(inputJsonPath1)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForMultiLine") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", LongType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1L,"France","1504679559","5"),
        Row(1L,"Germany","1504679359","6"),
        Row(1L,"Italy","1504679459","7"),
        Row(1L,"Belgium","1504679659","8")
      )),
      StructType(expectedSchema)
    )

    val df = JSONSparkLoader()
      .multiLine
      .load(inputJsonPath2)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

}
