import org.apache.spark._
val sc = new SparkContext(new SparkConf())

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

val posDisplaySize = 3
val posCtrSize = 26
val posDisplayUnit = 1000.0
val posCtrUnit = 0.001

def calculateCombFeatureWeightIdx(arr1_idx: Int, arr2_idx: Int, arr2_len: Int): Int = {
  if (arr1_idx == -1 || arr2_idx == -1) {
    return -1
  }
  arr1_idx * arr2_len + arr2_idx
}

def getDisplayCtrIdx(displaySize:Int, ctrSize:Int, displayUnit:Double,
                     ctrUnit:Double, display:Double, click:Double): (Int,Int) ={
  var ctrIdx = -1
  var displayIdx = -1
  if (display <= 0.0) {
    ctrIdx = ctrSize - 1
    displayIdx = displaySize - 1
  }
  else {
    displayIdx = (display / displayUnit).toInt
    if (displayIdx > displaySize - 2) {
      displayIdx = displaySize - 2
    }
    val ctr = click / display
    ctrIdx = (ctr / ctrUnit).toInt
    if (ctrIdx > ctrSize - 2) {
      ctrIdx = ctrSize - 2
    }
  }
  (displayIdx, ctrIdx)
}
/*
val rawTestData = sc.textFile("tracelog/parse-/2016-11-05/")
val recordsTest = rawTestData.map(line => line.split("\\|"))
val testData = recordsTest.filter(x => x.length == 20 && x(8) == "1" && x(15).takeRight(1)=="}")
val test = testData.map{r=>
  implicit val formats = DefaultFormats
  val label = r(1).toDouble
  val posDisplay = (parse(r(15)) \ "posDisplay").extract[Double]
  val posClick = (parse(r(15)) \ "posClick").extract[Double]
  val idea = (parse(r(15)) \ "ideaId").extract[String]
  val ad = (parse(r(15)) \ "adId").extract[String]
  val slotId = (parse(r(15)) \ "slotId").extract[String]
  val hour = (parse(r(15)) \ "queryTime_hour").extract[String].toInt
  val posDisplayCtrPair = getDisplayCtrIdx(posDisplaySize, posCtrSize, posDisplayUnit, posCtrUnit, posDisplay,posClick)
  val posDisplayPosCtrIdx = calculateCombFeatureWeightIdx(posDisplayCtrPair._1, posDisplayCtrPair._2, posCtrSize)
  (label, posDisplay, posClick, idea, ad, posDisplayPosCtrIdx, slotId, hour)
}.repartition(1000).cache()
val test1 = test.filter(x=>x._5=="2016103118321204365" && x._7=="2-4")
val test2 = test1.map(x=>((x._4,x._5,(x._3/10000).toInt,x._7,x._8),(x._1,1))).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2)).sortBy(_._1._5)
test2.take(300).foreach(println)

val test4 = rawTestData.filter(_.contains("2016103118321204365"))
*/

val rawTestData = sc.textFile("tracelog/v2/join_data/2016-11-05/")
val recordsTest = rawTestData.map(line => line.split("\t"))
val testData = recordsTest.filter(x => x.length == 2 && x(1).takeRight(1)=="}")
val test = testData.map{r=>
  implicit val formats = DefaultFormats
  val label = r(0).toDouble
  val posDisplay = (parse(r(1)) \ "posDisplay2").extract[Double]
  val posClick = (parse(r(1)) \ "posClick2").extract[Double]
  val idea = (parse(r(1)) \ "ideaId").extract[String]
  val ad = (parse(r(1)) \ "adId").extract[String]
  val slotId = (parse(r(1)) \ "slotId").extract[String]
  val hour = (parse(r(1)) \ "queryTime_hour").extract[String].toInt
  val posDisplayCtrPair = getDisplayCtrIdx(posDisplaySize, posCtrSize, posDisplayUnit, posCtrUnit, posDisplay,posClick)
  val posDisplayPosCtrIdx = calculateCombFeatureWeightIdx(posDisplayCtrPair._1, posDisplayCtrPair._2, posCtrSize)
  (label, posDisplay, posClick, idea, ad, posDisplayPosCtrIdx, slotId, hour)
}.repartition(1000).cache()
val test1 = test.filter(x=>x._5=="2016102411321192295" && x._7=="2-4")
val test2 = test1.map(x=>((x._4,x._5,(x._3 * 1000/x._2).toInt,x._7,x._8),(x._1,1))).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2)).sortBy(_._1._5)
test2.take(300).foreach(println)

val test4 = rawTestData.filter(_.contains("2016103118321204365"))

import scala.sys.process._
Seq("hadoop","fs","-ls","tracelog/v2/join_data").!

val data = sc.textFile("log/2016-11-06/*/*/display-*").map(_.split("\\|"))
val dianping = data.filter(_(27)=="dianping")
val dd = dianping.map(x=>(x(0).substring(11,13),(x(6).toDouble,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).collect()
