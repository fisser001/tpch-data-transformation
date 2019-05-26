package com.masterarbeit.scala.transformation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

//This Job was used to convert the utilization result for further analysis and visualization
object UtilizationJob {

  def main(args: Array[String]) {

    //Create Spark Session
    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .getOrCreate()

    //set spark configuration
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    spark.conf.set("spark.sql.orc.enabled", false)

    //Define path information to the raw utilization data
    val pathDrillSingleNormal1 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf1\\sf1_single_stats_normal\\*.csv"
    val pathDrillSingleDenormal1 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf1\\sf1_single_stats_denormal\\*.csv"
    val pathDrillSingleStar1 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf1\\sf1_single_stats_star\\*.csv"

    val pathDrillMultipleNormal1 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf1\\sf1_multiple_stats_normal\\*.csv"
    val pathDrillMultipleDeormal1 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf1\\sf1_multiple_stats_denormal\\*.csv"
    val pathDrillMultipleStar1 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf1\\sf1_multiple_stats_star\\*.csv"

    val pathDrillSingleNormal3 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf3\\sf3_single_stats_normal\\*.csv"
    val pathDrillSingleDenormal3 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf3\\sf3_single_stats_denormal\\*.csv"
    val pathDrillSingleStar3 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf3\\sf3_single_stats_star\\*.csv"

    val pathDrillMultipleNormal3 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf3\\sf3_multiple_stats_normal\\*.csv"
    val pathDrillMultipleDeormal3 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf3\\sf3_multiple_stats_denormal\\*.csv"
    val pathDrillMultipleStar3 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf3\\sf3_multiple_stats_star\\*.csv"


    val pathDrillSingleNormal10 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf10\\sf10_single_stats_normal\\*.csv"
    val pathDrillSingleDenormal10 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf10\\sf10_single_stats_denormal\\*.csv"
    val pathDrillSingleStar10 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf10\\sf10_single_stats_star\\*.csv"

    val pathDrillMultipleNormal10 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf10\\sf10_multiple_stats_normal\\*.csv"
    val pathDrillMultipleDeormal10 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf10\\sf10_multiple_stats_denormal\\*.csv"
    val pathDrillMultipleStar10 = "C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\presto\\sf10\\sf10_multiple_stats_star\\*.csv"

    //define schema of utilization data
    val schemaRaw = StructType(Array(
      StructField("raw", StringType, true)))

     import spark.implicits._

    //define UDFs
    val replace = udf((data: String , rep : String) => data.replaceAll(rep, ","))
    spark.udf.register("get_file_name", (path: String) => path.split("/").last.replaceAll(".csv_", "_").split("\\.").head)

    //Read the utilization data from path
    val data: DataFrame = spark.read.format("csv")
      .option("header", "false")
     // .option("inferSchema", "true")
      //.option("delimiter", " ")
      .schema(schemaRaw)
      //.load(pathDrillSingleNormal1)
      .load(pathDrillSingleNormal1,pathDrillSingleDenormal1,pathDrillSingleStar1,
        pathDrillMultipleNormal1,pathDrillMultipleDeormal1,pathDrillMultipleStar1,
        pathDrillSingleNormal3,pathDrillSingleDenormal3,pathDrillSingleStar3,
        pathDrillMultipleNormal3,pathDrillMultipleDeormal3, pathDrillMultipleStar3,
        pathDrillSingleNormal10, pathDrillSingleDenormal10,pathDrillSingleStar10,
        pathDrillMultipleNormal10, pathDrillMultipleDeormal10,pathDrillMultipleStar10)
      .filter(!$"raw".startsWith("CONTAINER"))

    val format = "HH:mm:ss.SSS"

    //Transform the data
    val newdata = data
      .withColumnRenamed(data.columns(0).toString,"raw")
      .withColumn("file_id", callUDF("get_file_name", input_file_name()))
      .withColumn("raw", regexp_replace($"raw" , lit("(  )"), lit("|")))
      .withColumn("raw", replace($"raw" , lit("[\\|]+")))
      .withColumn("containerid", trim(split(col("raw"), ",").getItem(0)))
      .withColumn("name", trim(split(col("raw"), ",").getItem(1)))
      .withColumn("cpu", trim(regexp_replace(split(col("raw"), ",").getItem(2),lit("%"),lit(""))))
      .withColumn("memusage", trim(split(col("raw"), ",").getItem(3)))
      .withColumn("memusage_percent", trim(regexp_replace(split(col("raw"), ",").getItem(4),lit("%"),lit(""))))
      .withColumn("netio", trim(split(col("raw"), ",").getItem(5)))
      .withColumn("blockio", trim(split(col("raw"), ",").getItem(6)))
      .withColumn("pids", trim(split(col("raw"), ",").getItem(7)))
      .withColumn("cpu_unit", lit("%"))
      .withColumn("memusage_percent_unit", lit("%"))
      .withColumn("memusage_current", trim(split(col("memusage"), "/").getItem(0)))
      .withColumn("memusage_current_unit", regexp_extract($"memusage_current","(GiB|MiB)",1))
      .withColumn("memusage_current", regexp_replace($"memusage_current" , lit("(GiB|MiB)"), lit("")))
      .withColumn("memusage_max", trim(split(col("memusage"), "/").getItem(1)))
      .withColumn("memusage_max_unit", regexp_extract($"memusage_max","(GiB|MiB)",1))
      .withColumn("memusage_max", regexp_replace($"memusage_max" , lit("(GiB|MiB)"), lit("")))
      .withColumn("netio_io_send", trim(split(col("netio"), "/").getItem(0)))
      .withColumn("netio_io_send_unit", regexp_extract($"netio_io_send","(MB|kB|GB|B)",1))
      .withColumn("netio_io_send", regexp_replace($"netio_io_send" , lit("(MB|kB|GB|B)"), lit("")))
      .withColumn("netio_io_receive", trim(split(col("netio"), "/").getItem(1)))
      .withColumn("netio_io_receive_unit", regexp_extract($"netio_io_receive","(MB|kB|GB|B)",1))
      .withColumn("netio_io_receive", regexp_replace($"netio_io_receive" , lit("(MB|kB|GB|B)"), lit("")))
      .withColumn("block_io_send", trim(split(col("blockio"), "/").getItem(0)))
      .withColumn("block_io_send_unit", regexp_extract($"block_io_send","(MB|kB|GB|B)",1))
      .withColumn("block_io_send", regexp_replace($"block_io_send" , lit("(MB|kB|GB|B)"), lit("")))
      .withColumn("block_io_receive", trim(split(col("blockio"), "/").getItem(1)))
      .withColumn("block_io_receive_unit", regexp_extract($"block_io_receive","(MB|kB|GB|B)",1))
      .withColumn("block_io_receive", regexp_replace($"block_io_receive" , lit("(MB|kB|GB|B)"), lit("")))
      .withColumn("tool", trim(split(col("file_id"), "_").getItem(0)))
      .withColumn("schema", trim(split(col("file_id"), "_").getItem(1)))
      .withColumn("sf", trim(split(col("file_id"), "_").getItem(3)))
      .withColumn("mode", trim(split(col("file_id"), "_").getItem(4)))

      .withColumn("executiondate", trim(split(col("file_id"), "_").getItem(6)))
      .withColumn("executiontime", trim(split(col("file_id"), "_").getItem(7)))
      .withColumn("executiontime", regexp_replace($"executiontime", "[^\\x00-\\x7F]", ":"))
      .withColumn("executiontime", regexp_replace($"executiontime", lit(":[0-9][0-9][0-9]"), concat(lit("."),regexp_extract($"executiontime", "[0-9][0-9][0-9]",0))))
        .select($"tool",$"schema",$"sf", $"mode",$"executiondate",$"executiontime",$"containerid", $"name", $"cpu", $"cpu_unit",$"memusage_current".cast(DoubleType),$"memusage_current_unit", $"memusage_max",$"memusage_max_unit", $"memusage_percent", $"memusage_percent_unit",
          $"netio_io_send".cast(DoubleType), $"netio_io_send_unit", $"netio_io_receive".cast(DoubleType),$"netio_io_receive_unit", $"block_io_send".cast(DoubleType),$"block_io_send_unit", $"block_io_receive".cast(DoubleType),$"block_io_receive_unit", $"pids")


    val newdata2: DataFrame = newdata
      .withColumn("memusage_current",when($"memusage_current_unit" === "MiB",$"memusage_current"/1000).otherwise($"memusage_current"))
      .withColumn("memusage_current",round($"memusage_current",2))
      .withColumn("memusage_current_unit",when($"memusage_current_unit" === "MiB",lit("GiB")).otherwise($"memusage_current_unit"))

      .withColumn("netio_io_send",when($"netio_io_send_unit" === "GB",$"netio_io_send"*1000).otherwise($"netio_io_send"))
      .withColumn("netio_io_send",when($"netio_io_send_unit" === "kB",$"netio_io_send"/1000).otherwise($"netio_io_send"))
      .withColumn("netio_io_send",when($"netio_io_send_unit" === "B",$"netio_io_send"/1000000).otherwise($"netio_io_send"))
      .withColumn("netio_io_send",round($"netio_io_send",2))
      .withColumn("netio_io_send_unit",when($"netio_io_send_unit" === "GB",lit("MB")).otherwise($"netio_io_send_unit"))
      .withColumn("netio_io_send_unit",when($"netio_io_send_unit" === "kB",lit("MB")).otherwise($"netio_io_send_unit"))
      .withColumn("netio_io_send_unit",when($"netio_io_send_unit" === "B",lit("MB")).otherwise($"netio_io_send_unit"))

      .withColumn("netio_io_receive",when($"netio_io_receive_unit" === "GB",$"netio_io_receive"*1000).otherwise($"netio_io_receive"))
      .withColumn("netio_io_receive",when($"netio_io_receive_unit" === "kB",$"netio_io_receive"/1000).otherwise($"netio_io_receive"))
      .withColumn("netio_io_receive",when($"netio_io_receive_unit" === "B",$"netio_io_receive"/1000000).otherwise($"netio_io_receive"))
      .withColumn("netio_io_receive",round($"netio_io_receive",2))
      .withColumn("netio_io_receive_unit",when($"netio_io_receive_unit" === "GB",lit("MB")).otherwise($"netio_io_receive_unit"))
      .withColumn("netio_io_receive_unit",when($"netio_io_receive_unit" === "kB",lit("MB")).otherwise($"netio_io_receive_unit"))
      .withColumn("netio_io_receive_unit",when($"netio_io_receive_unit" === "B",lit("MB")).otherwise($"netio_io_receive_unit"))

      .withColumn("block_io_send",when($"block_io_send_unit" === "GB",$"block_io_send"*1000).otherwise($"block_io_send"))
      .withColumn("block_io_send",when($"block_io_send_unit" === "kB",$"block_io_send"/1000).otherwise($"block_io_send"))
      .withColumn("block_io_send",when($"block_io_send_unit" === "B",$"block_io_send"/1000000).otherwise($"block_io_send"))
      .withColumn("block_io_send",round($"block_io_send",2))
      .withColumn("block_io_send_unit",when($"block_io_send_unit" === "GB",lit("MB")).otherwise($"block_io_send_unit"))
      .withColumn("block_io_send_unit",when($"block_io_send_unit" === "kB",lit("MB")).otherwise($"block_io_send_unit"))
      .withColumn("block_io_send_unit",when($"block_io_send_unit" === "B",lit("MB")).otherwise($"block_io_send_unit"))

      .withColumn("block_io_receive",when($"block_io_receive_unit" === "GB",$"block_io_receive"*1000).otherwise($"block_io_receive"))
      .withColumn("block_io_receive",when($"block_io_receive_unit" === "kB",$"block_io_receive"/1000).otherwise($"block_io_receive"))
      .withColumn("block_io_receive",when($"block_io_receive_unit" === "B",$"block_io_receive"/1000000).otherwise($"block_io_receive"))
      .withColumn("block_io_receive",round($"block_io_receive",2))
      .withColumn("block_io_receive_unit",when($"block_io_receive_unit" === "GB",lit("MB")).otherwise($"block_io_receive_unit"))
      .withColumn("block_io_receive_unit",when($"block_io_receive_unit" === "kB",lit("MB")).otherwise($"block_io_receive_unit"))
      .withColumn("block_io_receive_unit",when($"block_io_receive_unit" === "B",lit("MB")).otherwise($"block_io_receive_unit"))

    val pathWrite = s"C:\\Daten\\Projekte\\Masterarbeit\\Ergebnisse\\statsPresto"

    //Write data to local file system
    newdata2
      .filter(length($"containerid") === 12)
      .na.fill("")
      .repartition(1)
      //.show
      .write
      .mode("Overwrite")
      .option("header", "true")
      .option("delimiter", "|")
      .format("csv")
      .save(pathWrite)

  }
}
