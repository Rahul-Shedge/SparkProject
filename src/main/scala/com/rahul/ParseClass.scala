package com.rahul
//import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
//import com.rahul.Joining

class ParseClass {
  def Parsing(TextFile:Dataset[String],spark: SparkSession):List[DataFrame] = {
    import spark.implicits._
    val NewFile=TextFile.toDF()
    val PartialParsed = NewFile.select(regexp_extract($"value","""^([^(\s|,)]+)""",1).alias("host"),
      regexp_extract($"value","""^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("timestamp"),
      regexp_extract($"value","""^.*\w+\s+([^\s]+)\s+HTTP""",1).as("path"),
      regexp_extract($"value","""^.*"\s(\d{3})""",1).cast("int").as("status"))

    val new_df=PartialParsed.withColumn("_tmp",split($"path","/")).select($"_tmp".getItem(1)as "cat1",
      $"_tmp".getItem(2)as "cat2")

    val JClass = new Joining()
    val DF:sql.DataFrame = JClass.DFJoin(PartialParsed:sql.DataFrame,new_df:sql.DataFrame)
    val data = DF.select("host","timestamp","cat1","cat2","status","path")
    val NotNullData = data.na.drop("all")
    val pageViewedPerUser = NotNullData.groupBy("host").count().toDF()
    val cat1df = NotNullData.groupBy("cat1").count().sort(desc("count")).toDF()
    val cat2df = NotNullData.groupBy("cat2").count().sort(desc("count")).toDF()
    val path1df = NotNullData.groupBy("path").count().sort(desc("count")).toDF()
    val status1df = NotNullData.groupBy("status").count().sort(desc("count")).toDF()
    import org.apache.spark.sql.{functions => F}
    val UniquePageViewDf = NotNullData.groupBy("path").agg(F.countDistinct("host").as("UniquePageViewDf")).toDF()
    val Cat1PageViewDf = NotNullData.groupBy("cat1").agg(F.countDistinct("path").as("Cat1PageViewDf")).toDF()
    val Cat2PageViewDf = NotNullData.groupBy("cat2").agg(F.countDistinct("path").as("Cat2PageViewDf")).toDF()
    List(pageViewedPerUser,cat1df,cat2df,path1df,status1df,UniquePageViewDf,Cat1PageViewDf,Cat2PageViewDf)
	  //List(pageViewedPerUser,cat1df)
  }
}
