package org.example

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder() /*.master("local[3]")*/
      .config("spark.sql.orc.enabled","false")
      .appName("test")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //    val warehousePath = "/tmp/iceberg/"
    val catalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)
    val df = spark.sparkContext.parallelize(Array(
      (1, "name1"),
      (2, "name1"),
      (3, "name1")
    )).toDF("id", "name")
    //      .write
    //      .parquet("/tmp/test1")
    val schema = SparkSchemaUtil.convert(df.schema)
    val name: TableIdentifier = TableIdentifier.of("default", "test_table1")

    val table = catalog.createTable(name, schema)
    df.write
      .option("write-format", "avro")
      .format("iceberg")
      .mode(SaveMode.Overwrite)
      .save("default.test_table1")

    //    // read the table
    //    spark.read.format("iceberg").load("default.test_table")
  }
}