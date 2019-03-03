package com.masterarbeit.scala.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}


object TransformationJob {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .getOrCreate()

    import spark.implicits._

    val customer_raw = "C:/HFTL/Masterarbeit/DWH/TPC_H/tpch_sf1/data_sf1/customer.csv"

    val customer = spark.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(customer_raw)
      .drop($"_c8")

    customer.show(2, false)
    println(customer.count)

    customer.write.mode("Overwrite")
      .format("orc")
      .option("compression", "zlib")
      .save("C:/HFTL/Masterarbeit/DWH/TPC_H/tpch_sf1/data_sf1/customer")

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

}