package com.zeotap.source.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.{File, PrintWriter}

class CSVSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

  val inputCSVPath1 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=19"
  val inputCSVPath2 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=18"
  val inputCSVPath3 : String = "src/test/resources/custom-input-format/yr=2021/mon=07/dt=17"

  def writeToFile(path: String, data: String): Unit = {
    val fileObject = new File(path)
    val printWriter = new PrintWriter(fileObject)

    printWriter.write(data)
    printWriter.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.forceMkdir(new File(inputCSVPath1))
    FileUtils.forceMkdir(new File(inputCSVPath2))
    FileUtils.forceMkdir(new File(inputCSVPath3))

    val csvString1 =
      """
        |a,b,1,2
        |c,d,3,4
        |e,f,5,6
        |""".stripMargin

    val csvString2 = csvString1.replace(",", ".")

    val csvString3 =
      """
        |column1,column2,column3,column4
        |a,b,1,2
        |c,d,3,4
        |e,f,5,6
        |""".stripMargin

    writeToFile(inputCSVPath1 + "/a.csv", csvString1)
    writeToFile(inputCSVPath2 + "/a.csv", csvString2)
    writeToFile(inputCSVPath3 + "/a.csv", csvString3)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(inputCSVPath1))
    FileUtils.forceDelete(new File(inputCSVPath2))
    FileUtils.forceDelete(new File(inputCSVPath3))
  }

  test("testForLoadPath") {
    val expectedSchema = List(
      StructField("_c0", StringType, true),
      StructField("_c1", StringType, true),
      StructField("_c2", StringType, true),
      StructField("_c3", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("a","b","1","2"),
        Row("c","d","3","4"),
        Row("e","f","5","6")
      )),
      StructType(expectedSchema)
    )

    val df = CSVSparkLoader()
      .load(inputCSVPath1)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "_c0")
  }

  test("testForInferSchema") {
    val expectedSchema = List(
      StructField("_c0", StringType, true),
      StructField("_c1", StringType, true),
      StructField("_c2", IntegerType, true),
      StructField("_c3", IntegerType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("a","b",1,2),
        Row("c","d",3,4),
        Row("e","f",5,6)
      )),
      StructType(expectedSchema)
    )

    val df = CSVSparkLoader()
      .inferSchema
      .load(inputCSVPath1)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "_c0")
  }

  test("testForSeparator") {
    val expectedSchema = List(
      StructField("_c0", StringType, true),
      StructField("_c1", StringType, true),
      StructField("_c2", StringType, true),
      StructField("_c3", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("a","b","1","2"),
        Row("c","d","3","4"),
        Row("e","f","5","6")
      )),
      StructType(expectedSchema)
    )

    val df = CSVSparkLoader()
      .separator(".")
      .load(inputCSVPath2)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "_c0")
  }

  test("testForHeader") {
    val expectedSchema = List(
      StructField("column1", StringType, true),
      StructField("column2", StringType, true),
      StructField("column3", StringType, true),
      StructField("column4", StringType, true)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("a","b","1","2"),
        Row("c","d","3","4"),
        Row("e","f","5","6")
      )),
      StructType(expectedSchema)
    )

    val df = CSVSparkLoader()
      .header
      .load(inputCSVPath3)
      .buildUnsafe(spark)

    assertDataFrameEquality(expectedDf, df, "column1")
  }

}
