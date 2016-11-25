import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf())

// val dateArr = Array("2016-07-13","2016-07-14","2016-07-15","2016-07-16","2016-07-17","2016-07-18","2016-07-19",
//  "2016-07-20","2016-07-21","2016-07-22","2016-07-23")
val dateArr = Array("2016-09-05","2016-09-06")
//val dateArr = Array("2016-07-21")
//val expArr = Array("default", "exp20")
val expArr = Array("exp0", "exp9")
val slotArr = Array("2-4","2-12","1-3","1-10","1-30")
val ideaArr = Array("i201609042148200162")
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
      for (ad <- ideaArr) {
        adsDebugBuf = adsDebugBuf :+ ad
        // ios adr
        val displayData = ideaData.filter { case(exp1,slot1,idea1,feeType,ad1,clickCharge,displayCharge,pctr) => exp1 == exp && slot1 == slot && idea1 == ad}
        val clickData = ideaData2.filter { case(exp1,slot1,idea1,feeType,ad1,clickCharge,displayCharge,pctr) => exp1 == exp && slot1 == slot && idea1 == ad}

        val d = displayData.count()
        val c = clickData.count()
        val ctr = c * 1.0 / d
        val p = displayData.map(x => x._8.toDouble).mean
        val dCharge = displayData.map(x => x._7.toDouble).sum()
        val cCharge = clickData.map(x => x._6.toDouble).sum()
        val cpm = (dCharge + cCharge) * 1.0 / d
        val cpc = (dCharge + cCharge) * 1.0 / c
        adsDebugBuf = adsDebugBuf :+ Array(d,c,ctr,p,cpm,cpc,dCharge,cCharge).mkString("_")
      }
    }
  }
  ideaData.unpersist()
  ideaDataRaw.unpersist()
}
println("adsDebugBuf\n")
println(adsDebugBuf.mkString("\n"))

import java.util.Date
import java.text.SimpleDateFormat

val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
val dt = new Date()
val suffix = dateFormat.format(dt)

sc.parallelize(adsDebugBuf).saveAsTextFile("training_pipeline/ideaDisplayDebug/"+suffix+"/adsDebugBuf")
