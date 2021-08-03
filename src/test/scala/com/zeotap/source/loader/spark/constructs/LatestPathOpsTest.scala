package com.zeotap.source.loader.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.source.loader.spark.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.source.loader.utils.DataPickupUtils
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class LatestPathOpsTest extends FunSuite with DataFrameSuiteBase {

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
    FileUtils.forceDelete(new File(inputAvroPath1))
    FileUtils.forceDelete(new File(inputAvroPath2))
    FileUtils.forceDelete(new File(inputAvroPath3))
  }

  test("getDateFieldsTest") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val actualDateFields = LatestPathOps.getDateFields(new Path(pathTemplate), new Path(pathTemplate))

    assert(actualDateFields.equals("yr=${YR}/mon=${MON}/dt=${DT}"))
  }

  test("getNonDateFieldsTest") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val actualNonDateFields = LatestPathOps.getNonDateFields(new Path(pathTemplate), new Path(pathTemplate))

    assert(actualNonDateFields.equals("src/test/resources/custom-input-format"))
  }

  test("getAllLatestPaths relativeToCurrentDate = true") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val fullPathTemplate = "file:" + new File(pathTemplate).getAbsolutePath
    println(fullPathTemplate)
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "18")
    val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)

    val expectedLatestPaths = List("src/test/resources/custom-input-format/yr=2021/mon=07/dt=17")
    val actualLatestPaths = LatestPathOps.getAllLatestPaths(new Path(fullPathTemplate), fileSystem, parameters, true).map(x => "src" + x.split("src")(1))

    assert(expectedLatestPaths == actualLatestPaths)
  }

  test("getAllLatestPaths relativeToCurrentDate = false") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val fullPathTemplate = "file:" + new File(pathTemplate).getAbsolutePath
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "18")
    val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)

    val expectedLatestPaths = List("src/test/resources/custom-input-format/yr=2021/mon=07/dt=19")
    val actualLatestPaths = LatestPathOps.getAllLatestPaths(new Path(fullPathTemplate), fileSystem, parameters, false).map(x => "src" + x.split("src")(1))

    assert(expectedLatestPaths == actualLatestPaths)
  }

  test("getPathsForPattern") {
    FileUtils.forceMkdir(new File("src/test/resources/custom-input-format/yr=2021/mon=07/dt=20"))
    FileUtils.forceMkdir(new File("src/test/resources/custom-input-format/yr=2021/mon=07/dt=21"))

    val pathTemplate = "src/test/resources/custom-input-format/yr=*/mon=*/dt=*"
    val fullPathTemplate = "file:" + new File(pathTemplate).getAbsolutePath
    val fileSystem = DataPickupUtils.getFileSystem(pathTemplate)

    val expectedPathsForPattern = List(
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=17",
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18",
      "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19"
    )
    val actualPathsForPattern = LatestPathOps.getPathsForPattern(fileSystem, fullPathTemplate).map(x => x.toString).map(x => "src" + x.split("src")(1)).toList

    assert(expectedPathsForPattern == actualPathsForPattern)
    FileUtils.forceDelete(new File("src/test/resources/custom-input-format/yr=2021/mon=07/dt=20"))
    FileUtils.forceDelete(new File("src/test/resources/custom-input-format/yr=2021/mon=07/dt=21"))
  }

  test("latestPathReadTest") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val fullPathTemplate = "file:" + new File(pathTemplate).getAbsolutePath
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "18")

    val expectedDataFrame = spark.read.format("avro").load(inputAvroPath1)
    import com.zeotap.source.loader.spark.constructs.DataFrameReaderOps._
    val actualDataFrame = spark.read.format("avro").latestPath(fullPathTemplate, parameters, false)

    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "DeviceId")
  }

  test("latestPathReadTest2") {
    val pathTemplate = "src/test/resources/custom-input-format/yr=${YR}/mon=${MON}/dt=${DT}"
    val fullPathTemplate = "file:" + new File(pathTemplate).getAbsolutePath
    val parameters = Map("YR" -> "2021", "MON" -> "07", "DT" -> "18")

    val expectedDataFrame = spark.read.format("avro").load(inputAvroPath3)
    import com.zeotap.source.loader.spark.constructs.DataFrameReaderOps._
    val actualDataFrame = spark.read.format("avro").latestPath(fullPathTemplate, parameters, true)

    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "DeviceId")
  }

}
