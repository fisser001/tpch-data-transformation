package com.masterarbeit.scala.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}


object TransformationJobReadData {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .getOrCreate()

    import spark.implicits._

   val customerORC = spark.read.format("orc").load("C:/Daten/Projekte/Masterarbeit/TPC_H/tool/2.17.3/dbgen/Debug/tpch_sf1/data_sf1/customer.orc")

    customerORC.show(2,false)
    println(customerORC)

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