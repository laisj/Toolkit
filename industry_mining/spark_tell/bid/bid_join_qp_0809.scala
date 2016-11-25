/**
  * Created by laisijia on 16/8/10.
  */

import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.json4s._
import org.json4s.jackson.JsonMethods._
implicit val formats = DefaultFormats

val bidsData = sc.textFile("log/2016-07-21/*/*/bids-*").cache()
val qpData = sc.textFile("log/2016-07-21/*/*/queryParam-*").cache()

val bidsCpcArr = bidsData.map{x=>x.split("\\|")}.filter{x=>x.length == 7}.filter{x=>x(4) != "4-3"}.filter{x=>parse(x(5)).children.map(x=>(x \ "feeType").values.toString).filter(x=>x=="CPM").length==0}.map(x=>(x(3),x))

val bidsCpmArr = bidsData.map{x=>x.split("\\|")}.filter{x=>x.length == 7}.filter{x=>x(4) != "4-3"}.filter{x=>parse(x(5)).children.map(x=>(x \ "feeType").values.toString).filter(x=>x=="CPC").length==0}.map(x=>(x(3),x))

val qpKey = qpData.map(x=>x.split("\\|")).filter(x=>x.length==22).map(x=>(x(3), x))
// val qpKey = qpData.map(x=>x.split("\\|")).filter(x=>x.length==24).map(x=>(x(3), x))
val cpcQ = qpKey.join(bidsCpcArr)
val cpmQ = qpKey.join(bidsCpmArr)

var xiafac = Array[AnyVal]()
var cpcc = Array[AnyVal]()
var cpmc = Array[AnyVal]()
for (t <- Array("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")) {
  xiafac = xiafac :+ qpKey.filter(x=>x._2(0).substring(11,13)==t).count()
  cpcc = cpcc :+ cpcQ.filter(x=>x._2._1(0).substring(11,13)==t).count()
  cpmc = cpmc :+ cpmQ.filter(x=>x._2._1(0).substring(11,13)==t).count()
}

// smm rtlog/2016-07-21/00/*/adengine/feature/
