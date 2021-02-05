package com.rahul
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



object Analysis {
  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Logger.getLogger("org").setLevel(Level.OFF)


  if (args.length==4){
      //val sc = new SparkContext(new SparkConf().set("spark master",args(0)).set("spark.app.name",args(1)))
      val spark = SparkSession.builder().appName(args(1)).master(args(0)).getOrCreate()
      val TextFile = spark.read.textFile(args(2))
      spark.sparkContext.setLogLevel("WARN")
      val pc = new ParseClass
      val dfs = pc.Parsing(TextFile,spark)
      // val OutputPath = args(3)

      dfs.head.coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"PagesViewedByUser")
      dfs(1).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"Cat1")
      dfs(2).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"Cat2")
      dfs(3).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"path1")
      dfs(4).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"status1")
      dfs(5).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"UniquePageView")
      dfs(6).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"Cat1PageView")
      dfs(7).coalesce(1).write.option("header","true").option("sep",",").mode("overwrite")
        .csv(args(3)+"Cat2PageView")
  }
    else {
    print("pls enter four arguments")}
  }

}
