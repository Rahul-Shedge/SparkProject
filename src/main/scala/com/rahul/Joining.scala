package com.rahul
import org.apache.spark.sql
import org.apache.spark.sql.functions.monotonically_increasing_id

class Joining {
  def DFJoin(PartialParsed:sql.DataFrame,new_df:sql.DataFrame):sql.DataFrame = {
    val df2 = new_df.withColumn("id",monotonically_increasing_id())
    val df1 = PartialParsed.withColumn("id1",monotonically_increasing_id())
    val data_df= df1.join(df2, df1("id1") === df2("id"))
    data_df.toDF()

  }

}
