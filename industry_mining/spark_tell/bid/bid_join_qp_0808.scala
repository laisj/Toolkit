import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf())

import org.json4s._
import org.json4s.jackson.JsonMethods._
implicit val formats = DefaultFormats

var xiafac = Array[AnyVal]()
var cpcc = Array[AnyVal]()
var cpmc = Array[AnyVal]()
for (t <- (0 to 23).map("%02d".format(_)).toArray) {
val data = sc.textFile("log/2016-07-21/*/"+t+"/bids-*").cache()
val data3 = data.map{x=>x.split("\\|")}.filter{x=>x.length == 7}.filter{x=>x(4) != "4-3"}.filter{x=>parse(x(5)).children.map(x=>(x \ "feeType").values.toString).filter(x=>x=="CPM").length==0}
val data4 = data3.map(x=>(x(3),x))

val data7 = data.map{x=>x.split("\\|")}.filter{x=>x.length == 7}.filter{x=>x(4) != "4-3"}.filter{x=>parse(x(5)).children.map(x=>(x \ "feeType").values.toString).filter(x=>x=="CPC").length==0}
val data8 = data7.map(x=>(x(3),x))

val data5 = sc.textFile("log/2016-07-21/*/"+t+"/queryParam-*").map(x=>x.split("\\|")).filter(x=>x.length==24).map(x=>(x(3), x)).cache()
val data6 = data5.join(data4)
val data9 = data5.join(data8)
xiafac = xiafac :+ data5.count()
cpcc = cpcc :+ data6.count()
cpmc = cpmc :+ data9.count()
}

// smm rtlog/2016-07-21/00/*/adengine/feature/
