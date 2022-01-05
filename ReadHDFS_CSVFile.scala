package org
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHDFS_CSVFile {
  def main(args: Array[String]): Unit = {
    val spark_conf=newSparkConf().setAppName("Read-CSV").setMaster("local[1]")
    val sc=newSparkContext(spark_conf)
    val spark_session=SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    //CSV File location in HDFS
    val hdfs_file_location="hdfs://localhost:9800/user/data/csv/sample.csv"
    // Reading from HDFS
    val username_df=readFromHDFS(hdfs_file_location,spark_session)
    username_df.printSchema()
    username_df.collect().foreach(println)

  }
  def readFromHDFS(file_location: String, spark_session:Spark_Session):DataFrame={
    val username.df=spark_session.read.format(source="csv")
      .option("delimiter",";")
      .option("header","true")
      .option("inferSchema","true")
      .load(file_location)
    username_df
    {
      // For Printing Header
      val header=hdfs_file_location.first()
      val csvRDDWithoutHeader=csvRDD.filter(_!=header)
      csvRDDWithoutHeader.take(10).foreach(println)

      //For printing limited number of columns
      val customerWithlimitedCols=csvRDDWithoutHeader.map(line=>{ val colArray=line.split(",")
        Array(colArray(0),colArray(2).mkString(";"))
      }).take(10).foreach(println)
    }
  }
}
