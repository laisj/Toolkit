import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf())

import org.json4s._
import org.json4s.jackson.JsonMethods._
implicit val formats = DefaultFormats

val data30 = sc.textFile("rtlog/2016-09-23/1*/*/adengine/bids")
//val data31 = sc.textFile("log/2016-08-31/*/*/bids-*")
// val data = sc.textFile("rtlog/2016-07-18/09/*/home/logs/adengine/bids")

val bidsAdArr30 = data30.map{x=>x.split("\\|")}.filter{x=>x.length == 7 && x(4) != "4-3" && parse(x(5)).children.map(x=>(x \ "adId").values.toString).contains("2016092314501141933")}.map(x=>(x(0).substring(0,13),1)).reduceByKey(_+_)
//val bidsAdArr31 = data31.map{x=>x.split("\\|")}.filter{x=>x.length == 7 && x(4) != "4-3" && parse(x(5)).children.map(x=>(x \ "adId").values.toString).contains("201606011439974343")}.map(x=>(x(0).substring(0,13),1)).reduceByKey(_+_)

bidsAdArr30.take(30)
//bidsAdArr31.take(30)