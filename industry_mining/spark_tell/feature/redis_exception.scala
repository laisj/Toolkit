import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.json4s._
import org.json4s.jackson.JsonMethods._

val featureData = sc.textFile("rtlog/2016-08-17/16/*/adengine/feature/")
featureData.filter{x=>x.split("\\|")(3)!="4-3"}.filter{x=>implicit val formats = DefaultFormats;(parse(x.split("\\|")(5)) \ "posDisplay").extract[Double]==0.0}.top(3).foreach{println}
featureData.filter{x=>x.split("\\|")(3)!="4-3" && x.split("\\|")(3)!="1-8" && x.split("\\|")(3)!="1-23"}.filter{x=>implicit val formats = DefaultFormats;(parse(x.split("\\|")(5)) \ "posDisplay").extract[Double]==0.0}.map{x=>((x.split("\\|")(3),x.split("\\|")(4)),1)}.reduceByKey(_+_).sortBy(-_._2).collect()

val parseData =  sc.textFile("tracelog/parse-/2016-08-16/")
val zCount = parseData.filter{x=>x.split("\\|")(10)!="4-3"}.filter{x=>implicit val formats = DefaultFormats;(parse(x.split("\\|")(15)) \ "posDisplay").extract[Double]==0.0}.count()
val zAdCount = parseData.filter{x=>x.split("\\|")(10)!="4-3"}.filter{x=>implicit val formats = DefaultFormats;(parse(x.split("\\|")(15)) \ "posDisplay").extract[Double]==0.0}.map(x=>((x.split("\\|")(10),x.split("\\|")(19)),1)).reduceByKey(_+_).sortBy(-_._2).collect()
