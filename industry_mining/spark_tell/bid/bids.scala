
import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.json4s._
import org.json4s.jackson.JsonMethods._
implicit val formats = DefaultFormats

val data = sc.textFile("log/2016-08-30/*/0*/bids-*")
// val data = sc.textFile("rtlog/2016-07-18/09/*/home/logs/adengine/bids")

val data3 = parse(data.first.split("\\|")(5))

for (d4 <- data3.children){
  println((d4 \ "qScore").values, (d4 \ "ideaId").values, (d4 \ "adId").values, (d4 \ "feeType").values, (d4 \ "ctr").values, (d4 \ "eCPM").values)
}

data3.children.foreach(x=>println(x.values))

data3.children(0).values.asInstanceOf[Map[String, String]]


val bidsData = sc.textFile("log/2016-08-0[5-7]/*/*/bids-*")
val qrData = sc.textFile("log/2016-08-0[5-7]/*/*/queryResult-*")

val bidsAdArr = bidsData.map{x=>x.split("\\|")}.filter{x=>x.length == 7 && x(4) != "4-3" && parse(x(5)).children.map(x=>(x \ "adId").values.toString).contains("2016080209351068991")}.map(x=>(x(0).substring(0,13),x))
bidsAdArr.cache()

val bidsTimePrice = bidsAdArr.map{case(x,y)=>
  val queue = parse(y(5)).children
  val pos = queue.map(xx=>(xx \ "adId").values).indexOf("2016080209351068991")
  val pctr = (queue(pos) \ "ctr").values.toString.toDouble
  var price = 500 * pctr
  if (pos + 1 < queue.length) {
    price = (queue(pos+1) \ "eCPM").values.toString.toDouble
  }
  (x, (price,1.0))
}

val priceBucket = bidsTimePrice.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))

val qrAdArr = qrData.map{x=>x.split("\\|")}.filter{x=>x.length == 22 && x(5) != "4-3" && x(4) == "2016080209351068991" }.map(x=>(x(0).substring(0,13),x))
qrAdArr.cache()

val bidsTimeCnt = bidsAdArr.map{case(x,y)=>(x,1)}.reduceByKey(_+_)
val qrTimeCnt = qrAdArr.map{case(x,y)=>(x,1)}.reduceByKey(_+_)


val clickData = sc.textFile("log/2016-08-0[5-7]/*/*/click-*")

val displayData = sc.textFile("log/2016-08-0[5-7]/*/*/display-*")

// 2016080120331068799

//import org.json4s._
//import org.json4s.jackson.JsonMethods._
//implicit val formats = DefaultFormats
//
//val bidsData = sc.textFile("log/2016-08-0[5-7]/*/*/bids-*").cache()
//val qrData = sc.textFile("log/2016-08-0[5-7]/*/*/queryResult-*").cache()
//
//val bidsAdArr = bidsData.map{x=>x.split("\\|")}.filter{x=>x.length == 7 && x(4) != "4-3" && parse(x(5)).children.map(x=>(x \ "adId").values.toString).contains("2016080120331068799")}.map(x=>(x(0).substring(0,13),x))
//bidsAdArr.cache()
//val qrAdArr = qrData.map{x=>x.split("\\|")}.filter{x=>x.length == 22 && x(5) != "4-3" && x(4) == "2016080120331068799" }.map(x=>(x(0).substring(0,13),x))
//qrAdArr.cache()
//
//val bidsTimeCnt = bidsAdArr.map{case(x,y)=>(x,1)}.reduceByKey(_+_)
//val qrTimeCnt = qrAdArr.map{case(x,y)=>(x,1)}.reduceByKey(_+_)

//
// smm rtlog/2016-07-21/00/*/adengine/feature/
