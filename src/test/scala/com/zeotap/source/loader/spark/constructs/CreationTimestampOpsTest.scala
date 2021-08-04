package com.zeotap.source.loader.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.cloudstorageutils.CloudStorePathMetaGenerator
import com.zeotap.source.loader.spark.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar.mock
import org.mockito.Mockito

import java.io.File

class CreationTimestampOpsTest extends FunSuite with DataFrameSuiteBase {

  val inputOrcPath: String = "src/test/resources/custom-input-format/orc"
  val inputAvroPath1 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19"
  val inputAvroPath2 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18"
  val inputAvroPath3 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=17"

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

    sampleDf.write.format("orc").save(inputOrcPath)
    sampleDf.write.format("avro").save(inputAvroPath1)

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

    sampleDf2.write.format("avro").save(inputAvroPath2)

    val testSchema3 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Gender", StringType, true)
    )

    val sampleDf3 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"France","1504679559","5","Male"),
        Row(1,"Germany","1504679359","6","Female"),
        Row(1,"Italy","1504679459","7","Female"),
        Row(1,"Belgium","1504679659","8","Male")
      )),
      StructType(testSchema3)
    )

    sampleDf3.write.format("avro").save(inputAvroPath3)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputOrcPath))
    FileUtils.forceDelete(new File(inputAvroPath1))
    FileUtils.forceDelete(new File(inputAvroPath2))
    FileUtils.forceDelete(new File(inputAvroPath3))
  }

  test("inputPathsArrayTest") {
    val dataFrame = spark.read.format("avro").load(List(inputAvroPath1, inputAvroPath2) : _*)

    val expectedInputPathsArray = Array("src/test/resources/custom-input-format/yr=2021/mon=07/dt=19/",
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18/")
    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    val actualInputPathsArray = dataFrame.getPathsArray.map(x => "src" + x.split("/src")(1))

    assert(expectedInputPathsArray.sorted.sameElements(actualInputPathsArray.sorted))
  }

  test("addRawTimestampColumnFromInputFilePathTest") {
    val dataFrame = spark.read.format("avro").load(inputAvroPath1)

    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("CREATED_TS_raw", StringType, true)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","1627929000"),
        Row(1,"2","India","1504679359","1627929000"),
        Row(1,"3","Spain","1504679459","1627929000"),
        Row(1,"4","India","1504679659","1627929000")
      )),
      StructType(expectedSchema)
    )

    val cloudStorePathMetaGenerator = mock[CloudStorePathMetaGenerator]

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    val inputPathsArray = dataFrame.getPathsArray
    val pathTsMap: Map[String, String] = dataFrame.withColumn("inputPath", input_file_name())
      .select("inputPath").distinct().collect.map(row => row.getString(0))
      .flatMap(x => Map(x -> "2021-08-03 00:00")).toMap

    Mockito.when(cloudStorePathMetaGenerator.partFileRawTsMapGenerator(inputPathsArray)).thenReturn(pathTsMap)
    val actualDataFrame = dataFrame.addRawTimestampColumnFromInputFilePath(cloudStorePathMetaGenerator)

    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "DeviceId")
  }

  test("appendRawTsToDataFrameTest(inputType = raw)") {
    val dataFrame = spark.read.format("avro").load(inputAvroPath1)

    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("CREATED_TS_raw", StringType, true)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","1627929000"),
        Row(1,"2","India","1504679359","1627929000"),
        Row(1,"3","Spain","1504679459","1627929000"),
        Row(1,"4","India","1504679659","1627929000")
      )),
      StructType(expectedSchema)
    )

    val cloudStorePathMetaGenerator = mock[CloudStorePathMetaGenerator]

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    val inputPathsArray = dataFrame.getPathsArray
    val pathTsMap: Map[String, String] = dataFrame.withColumn("inputPath", input_file_name())
      .select("inputPath").distinct().collect.map(row => row.getString(0))
      .flatMap(x => Map(x -> "2021-08-03 00:00")).toMap

    Mockito.when(cloudStorePathMetaGenerator.partFileRawTsMapGenerator(inputPathsArray)).thenReturn(pathTsMap)
    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    val actualDataFrame = dataFrame.appendRawTsToDataFrame("raw")(cloudStorePathMetaGenerator)

    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "DeviceId")
  }

  test("vanilla appendRawTsToDataFrameTest(inputType = preprocess)") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("CREATED_TS_raw", StringType, true)
    )

    val dataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","1627929000"),
        Row(1,"2","India","1504679359","1627929000"),
        Row(1,"3","Spain","1504679459","1627929000"),
        Row(1,"4","India","1504679659","1627929000")
      )),
      StructType(schema)
    )

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    val actualDataFrame = dataFrame.appendRawTsToDataFrame("preprocess")

    assertDataFrameEquality(dataFrame, actualDataFrame, "DeviceId")
  }

  test("appendRawTsToDataFrameTest(inputType = preprocess) when dataFrame does not have CREATED_TS_raw column") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val dataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema)
    )

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    assertThrows[NoSuchElementException](dataFrame.appendRawTsToDataFrame("preprocess"))
  }

  test("vanilla appendRawTsToDataFrameTest(inputType = tube)") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("timestamp", StringType, true)
    )

    val dataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","1627929000"),
        Row(1,"2","India","1504679359","1627929000"),
        Row(1,"3","Spain","1504679459","1627929000"),
        Row(1,"4","India","1504679659","1627929000")
      )),
      StructType(schema)
    )

    val expectedDataFrame = dataFrame.withColumn("CREATED_TS_raw", col("timestamp"))

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    val actualDataFrame = dataFrame.appendRawTsToDataFrame("tube")

    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "DeviceId")
  }

  test("appendRawTsToDataFrameTest(inputType = tube) when dataFrame does not have timestamp column") {
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val dataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema)
    )

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    assertThrows[NoSuchElementException](dataFrame.appendRawTsToDataFrame("tube"))
  }

  test("appendRawTsToDataFrameTest wrong inputType") {
    val dataFrame = spark.emptyDataFrame

    import com.zeotap.source.loader.spark.constructs.DataFrameOps._
    assertThrows[IllegalArgumentException](dataFrame.appendRawTsToDataFrame("wrongInputType"))
  }

}
