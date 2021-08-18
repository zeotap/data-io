package com.zeotap.source.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class ParquetSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

  val inputParquetPath1 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19"
  val inputParquetPath2 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18"

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

    sampleDf.write.format("parquet").save(inputParquetPath1)

    val testSchema2 = List(
      StructField("Common_DataPartnerID2", IntegerType, true),
      StructField("Demographic_Country2", StringType, true),
      StructField("Common_TS2", StringType, true),
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

    sampleDf2.write.format("parquet").save(inputParquetPath2)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputParquetPath1))
    FileUtils.forceDelete(new File(inputParquetPath2))
  }

  test("testForLoadPath") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(expectedSchema)
    )

    val df = ParquetSparkLoader()
      .load(inputParquetPath1)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "DeviceId")
  }

  test("testForMergeSchema") {
    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("Common_DataPartnerID2", IntegerType, true),
      StructField("Demographic_Country2", StringType, true),
      StructField("Common_TS2", StringType, true),
      StructField("dt", IntegerType, true)

    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559",null,null,null,19),
        Row(1,"2","India","1504679359",null,null,null,19),
        Row(1,"3","Spain","1504679459",null,null,null,19),
        Row(1,"4","India","1504679659",null,null,null,19),
        Row(null,"5",null,null,1,"France","1504679559",18),
        Row(null,"6",null,null,1,"Germany","1504679359",18),
        Row(null,"7",null,null,1,"Italy","1504679459",18),
        Row(null,"8",null,null,1,"Belgium","1504679659",18)
      )),
      StructType(expectedSchema)
    )

    val df = ParquetSparkLoader()
      .mergeSchema
      .load("src/test/resources/custom-input-format/yr=2021/mon=07")
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df,  "DeviceId")
  }

}
