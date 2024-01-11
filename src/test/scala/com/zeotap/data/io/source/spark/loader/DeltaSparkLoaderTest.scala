package com.zeotap.data.io.source.spark.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import java.io.File

class DeltaSparkLoaderTest extends FunSuite with DataFrameSuiteBase {

    val inputDeltaPath1: String = "src/test/resources/custom-input-format/yr=2023/mon=02/dt=01"
    val inputDeltaPath2: String = "src/test/resources/custom-input-format/yr=2023/mon=02/dt=02"

    override def beforeAll(): Unit = {
        System.setProperty("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        System.setProperty("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        super.beforeAll()

        val testSchema = List(
            StructField("Common_DataPartnerID", IntegerType, true),
            StructField("DeviceId", StringType, true),
            StructField("Demographic_Country", StringType, true),
            StructField("Common_TS", StringType, true)
        )

        val sampleDf = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(
                Row(1, "1", "India", "1504679559"),
                Row(1, "2", "India", "1504679359"),
                Row(1, "3", "Spain", "1504679459"),
                Row(1, "4", "India", "1504679659")
            )),
            StructType(testSchema)
        )

        sampleDf.write.format("delta").save(inputDeltaPath1)

        val testSchema2 = List(
            StructField("Common_DataPartnerID2", IntegerType, true),
            StructField("Demographic_Country2", StringType, true),
            StructField("Common_TS2", StringType, true),
            StructField("DeviceId", StringType, true)
        )

        val sampleDf2 = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(
                Row(1, "France", "1504679559", "5"),
                Row(1, "Germany", "1504679359", "6"),
                Row(1, "Italy", "1504679459", "7"),
                Row(1, "Belgium", "1504679659", "8")
            )),
            StructType(testSchema2)
        )

        sampleDf2.write.format("delta").save(inputDeltaPath2)
    }

    override def afterAll(): Unit = {
        super.afterAll()
        FileUtils.forceDelete(new File(inputDeltaPath1))
        FileUtils.forceDelete(new File(inputDeltaPath2))
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
                Row(1, "1", "India", "1504679559"),
                Row(1, "2", "India", "1504679359"),
                Row(1, "3", "Spain", "1504679459"),
                Row(1, "4", "India", "1504679659")
            )),
            StructType(expectedSchema)
        )

        val df = DeltaSparkLoader()
            .load(inputDeltaPath1)
            .buildUnsafe(spark)

        assertDataFrameEquality(expectedDf, df, "DeviceId")
    }


    test("testForLoadPath2") {
        val expectedSchema = List(
            StructField("Common_DataPartnerID2", IntegerType, true),
            StructField("Demographic_Country2", StringType, true),
            StructField("Common_TS2", StringType, true),
            StructField("DeviceId", StringType, true)
        )

        val expectedDf = spark.createDataFrame(
            spark.sparkContext.parallelize(Seq(
                Row(1, "France", "1504679559", "5"),
                Row(1, "Germany", "1504679359", "6"),
                Row(1, "Italy", "1504679459", "7"),
                Row(1, "Belgium", "1504679659", "8")
            )),
            StructType(expectedSchema)
        )

        val df = DeltaSparkLoader()
            .load(inputDeltaPath2)
            .buildUnsafe(spark)

        assertDataFrameEquality(expectedDf, df, "DeviceId")
    }
}
