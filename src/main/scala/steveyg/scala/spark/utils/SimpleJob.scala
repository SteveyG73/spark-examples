package steveyg.scala.spark.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

trait SimpleJob extends App {


  def writeToCSV(df:DataFrame, outfile:String, subDir:String) : Unit = {
    df.repartition(1).write.mode(SaveMode.Overwrite).csv(outfile+"/"+subDir)
  }
}
