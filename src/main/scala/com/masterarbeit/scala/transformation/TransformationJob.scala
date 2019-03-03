package com.masterarbeit.scala.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object TransformationJob {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .getOrCreate()

    import spark.implicits._

    val customer_raw = "C:\\Daten\\Projekte\\Masterarbeit\\TPC_H\\tool\\2.17.3\\dbgen\\Debug\\tpch_sf1\\data_sf1\\customer.csv"
    val customer_schema = StructType(Array(
      StructField("c_custkey", StringType, true),
      StructField("c_name", StringType, true),
      StructField("c_address", StringType, true),
      StructField("c_nationkey", StringType, true),
      StructField("c_phone", StringType, true),
      StructField("c_acctbal", DoubleType, true),
      StructField("c_mktsegment", StringType, true),
      StructField("c_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val region_raw = "C:\\Daten\\Projekte\\Masterarbeit\\TPC_H\\tool\\2.17.3\\dbgen\\Debug\\tpch_sf1\\data_sf1\\region.csv"
    val region_schema = StructType(Array(
      StructField("r_regionkey", StringType, true),
      StructField("r_name", StringType, true),
      StructField("r_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val nation_raw = "C:\\Daten\\Projekte\\Masterarbeit\\TPC_H\\tool\\2.17.3\\dbgen\\Debug\\tpch_sf1\\data_sf1\\nation.csv"
    val nation_schema = StructType(Array(
      StructField("n_nationkey", StringType, true),
      StructField("n_name", StringType, true),
      StructField("n_regionkey", StringType, true),
      StructField("n_comment", StringType, true),
      StructField("last_col", StringType, true)))

    val customerDf = readData(spark,customer_raw,customer_schema)
    val regionDf = readData(spark,region_raw,region_schema)
    val nationDf = readData(spark,nation_raw,nation_schema)

    val denormDf = denormJoin(spark, customerDf,nationDf, regionDf)

    /*customer.write.mode("Overwrite")
      .format("orc")
      .option("compression", "zlib")
      .save("C:/HFTL/Masterarbeit/DWH/TPC_H/tpch_sf1/data_sf1/customer")*/

    val sourceDf:DataFrame = Seq(
      (
        "value1",
        "value2",
        "value3",
        "value4"
      )
    ).toDF(
      "col1",
      "col2",
      "col3",
      "col4"
    )

   sourceDf.show(false)
  }

  def readData(spark:SparkSession, dataPath: String,schemaOfCsv: StructType):DataFrame = {
    import spark.implicits._
    val data: DataFrame = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", "|")
      .schema(schemaOfCsv)
      .load(dataPath)
      .drop($"last_col")

    data.show(2, false)
    println(data.count)
    return data
  }

  def denormJoin(spark:SparkSession, customer: DataFrame,nation: DataFrame, region: DataFrame):DataFrame = {
    import spark.implicits._

    val joined_df = customer
      .join(nation, col("c_nationkey") === col("n_nationkey"), "left")
      .join(region, col("n_regionkey") === col("r_regionkey"), "left")

    joined_df.show(2, false)
    println(joined_df.count)
    return joined_df
  }


}