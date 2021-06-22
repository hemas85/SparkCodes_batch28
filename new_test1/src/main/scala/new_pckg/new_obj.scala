package new_pckg

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import scala.io.Source


object new_obj {
  
  def main(args:Array[String]):Unit={
    println("Zeyobron")
    
         val conf = new SparkConf().setAppName("data").setMaster("local[*]")
		val sc =  new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
    val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val html = Source.fromURL("https://randomuser.me/api/0.8/?results=1")
val s = html.mkString
//println(s)

val urlrdd= sc.parallelize(List(s))

val urldf = spark.read.json(urlrdd)

urldf.show()
urldf.printSchema()

val df1=urldf
.withColumn("results",explode(col("results")))
.select("nationality","seed","version","results.user.username",
    "results.user.cell","results.user.dob","results.user.email",
    "results.user.gender","results.user.location.city","results.user.location.state",
    "results.user.location.street","results.user.location.zip","results.user.md5","results.user.name.first",
    "results.user.name.last","results.user.name.title","results.user.password","results.user.phone",
    "results.user.picture.large","results.user.picture.medium","results.user.picture.thumbnail",
    "results.user.registered","results.user.salt","results.user.sha1","results.user.sha256")
    
    df1.show()

  }
}