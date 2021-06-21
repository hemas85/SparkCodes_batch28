package new_pckg

import org.apache.spark._
		import org.apache.spark.sql._
	import org.apache.spark.sql.types._
		import org.apache.spark.sql.Row
		import org.apache.spark.sql.functions._

object new_obj {
  
  def main(args:Array[String]):Unit={
    println("Zeyobron")
    
         val conf = new SparkConf().setAppName("data").setMaster("local[*]")
		val sc =  new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
    val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
  }
}