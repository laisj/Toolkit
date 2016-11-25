import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf().setAppName("topad_displayclick"))

import java.util.Date
import java.text.SimpleDateFormat

val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
val dt = new Date()
val suffix = dateFormat.format(dt)

val dt_yesterday = new Date(System.currentTimeMillis()-24*60*60*1000)
val dateArr = Array(new SimpleDateFormat("yyyy-MM-dd").format(dt_yesterday))

val expArr = Array("default")
val slotArr = Array("2-4","2-12","1-3","1-10","1-30")

// head ad on top
var topAds = Array[String]()
var topAdsCpc = Array[String]()
var adMap = Map[String,Array[(String,String)]]()

for (date <- dateArr) {
  topAds = topAds :+ date
  val displayData = sc.textFile("log/"+date+"/*/*/display-*")
  val clickData = sc.textFile("log/"+date+"/*/*/click-*")
  val ideaData = displayData.map{ x=>
    val xarr = x.split("\\|")
    val exp = xarr(17)
    val slot = xarr(10)
    val idea = xarr(14)
    val feeType = xarr(12)
    val ad = xarr(5)
    val clickCharge = xarr(6)
    val displayCharge = xarr(7)
    (exp,slot,idea,feeType,ad,clickCharge,displayCharge)
  }
  ideaData.cache()
  for (exp <- expArr) {
    topAds = topAds :+ exp
    topAdsCpc = topAdsCpc :+ exp
    for (slot <- slotArr) {
      topAds = topAds :+ slot
      topAdsCpc = topAdsCpc :+ slot
      val ideaFilteredData = ideaData.filter { x => x._1 == exp && x._2 == slot }.map { x =>
        ((x._3,x._4,x._5), 1) }.reduceByKey(_ + _).sortBy(-_._2).take(10)
      topAds = topAds :+ ideaFilteredData.mkString("\t")
      topAdsCpc = topAdsCpc :+ ideaFilteredData.filter(x=>x._1._2=="1").mkString("\t")
      adMap += (date + "_" + exp + "_" + slot -> ideaFilteredData.filter(x=>x._1._2=="1").map(x=>(x._1._1,x._1._3)))
    }
  }
  ideaData.unpersist()
}

// cpc accurracy
var adsDebugBuf = Array[String]()

for (date <- dateArr) {
  adsDebugBuf = adsDebugBuf :+ date
  val displayData = sc.textFile("log/"+date+"/*/*/display-*")
  val clickData = sc.textFile("log/"+date+"/*/*/click-*")
  val ideaDataRaw = displayData.map{ x=>
    val xarr = x.split("\\|")
    val exp = xarr(17)
    val slot = xarr(10)
    val idea = xarr(14)
    val feeType = xarr(12)
    val ad = xarr(5)
    val clickCharge = xarr(6)
    val displayCharge = xarr(7)
    val pctr = xarr(20)
    (exp,slot,idea,feeType,ad,clickCharge,displayCharge,pctr)
  }
  ideaDataRaw.cache()
  val ideaData = ideaDataRaw.filter(x=>x._8 != "-")
  ideaData.cache()
  adsDebugBuf = adsDebugBuf :+ ("raw:" + ideaDataRaw.count().toString + ",filtered:" + ideaData.count().toString)

  val ideaData2Raw = clickData.map{ x=>
    val xarr = x.split("\\|")
    val exp = xarr(17)
    val slot = xarr(10)
    val idea = xarr(14)
    val feeType = xarr(12)
    val ad = xarr(5)
    val clickCharge = xarr(6)
    val displayCharge = xarr(7)
    val pctr = xarr(20)
    (exp,slot,idea,feeType,ad,clickCharge,displayCharge,pctr)
  }
  ideaData2Raw.cache()
  val ideaData2 = ideaData2Raw.filter(x=>x._8 != "-")
  ideaData2.cache()
  adsDebugBuf = adsDebugBuf :+ ("raw:" + ideaData2Raw.count().toString + ",filtered:" + ideaData2.count().toString)

  for (exp <- expArr) {
    adsDebugBuf = adsDebugBuf :+ exp
    for (slot <- slotArr) {
      adsDebugBuf = adsDebugBuf :+ slot
      for (ad <- adMap(date + "_" + exp + "_" + slot)) {
        adsDebugBuf = adsDebugBuf :+ (ad._1 + "_" + ad._2)
        // ios adr
        val displayData = ideaData.filter { case(exp1,slot1,idea1,feeType,ad1,clickCharge,displayCharge,pctr) => exp1 == exp && slot1 == slot && idea1 == ad._1}
        val clickData = ideaData2.filter { case(exp1,slot1,idea1,feeType,ad1,clickCharge,displayCharge,pctr) => exp1 == exp && slot1 == slot && idea1 == ad._1}
        //val iosData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad && x._4 == 1}
        val d = displayData.count()
        val c = clickData.count()
        val ctr = c * 1.0 / d
        val p = displayData.map(x => x._8.toDouble).mean
        val dCharge = displayData.map(x => x._7.toDouble).sum()
        val cCharge = clickData.map(x => x._6.toDouble).sum()
        val cpm = (dCharge + cCharge) * 1.0 / d
        val cpc = (dCharge + cCharge) * 1.0 / c
        adsDebugBuf = adsDebugBuf :+ Array(d,c,ctr,p,cpm,cpc,dCharge,cCharge).mkString("\t")
      }
    }
  }
  ideaData.unpersist()
  ideaDataRaw.unpersist()
}
println("topAds\n")
println(topAds.mkString("\n"))
println("topAdsCpc\n")
println(topAdsCpc.mkString("\n"))
println("adsDebugBuf\n")
println(adsDebugBuf.mkString("\n"))

sc.parallelize(topAds).saveAsTextFile("training_pipeline/displayDebug/"+suffix+"/topAds")
sc.parallelize(topAdsCpc).saveAsTextFile("training_pipeline/displayDebug/"+suffix+"/topAdsCpc")
sc.parallelize(adsDebugBuf).saveAsTextFile("training_pipeline/displayDebug/"+suffix+"/adsDebugBuf")
