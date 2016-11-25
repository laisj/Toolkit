import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

// val dateArr = Array("2016-07-13","2016-07-14","2016-07-15","2016-07-16","2016-07-17","2016-07-18","2016-07-19",
//  "2016-07-20","2016-07-21","2016-07-22","2016-07-23")
val dateArr = Array("2016-09-10","2016-09-11","2016-09-12","2016-09-13")
//val dateArr = Array("2016-07-21")
//val expArr = Array("default", "exp20")
val expArr = Array("default","exp1")
val slotArr = Array("2-4","2-12")
val adArr = Array("2016091010041121657")

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
    val timeStr = xarr(0).substring(11,13)
    (exp,slot,idea,feeType,ad,clickCharge,displayCharge,pctr,timeStr)
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
    val timeStr = xarr(0).substring(11,13)
    (exp,slot,idea,feeType,ad,clickCharge,displayCharge,pctr,timeStr)
  }
  ideaData2Raw.cache()
  val ideaData2 = ideaData2Raw.filter(x=>x._8 != "-")
  ideaData2.cache()
  adsDebugBuf = adsDebugBuf :+ ("raw:" + ideaData2Raw.count().toString + ",filtered:" + ideaData2.count().toString)

  for (exp1 <- expArr) {
    adsDebugBuf = adsDebugBuf :+ exp1
    for (slot <- slotArr) {
      adsDebugBuf = adsDebugBuf :+ slot
      for (ad <- adArr) {
        adsDebugBuf = adsDebugBuf :+ ad
        for (t <- Array("00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23")) {
          val displayData = ideaData.filter { case (p_exp, slot1, idea1, feeType, ad1, clickCharge, displayCharge, pctr, timeStr) => p_exp == exp1 && slot1 == slot && ad1 == ad && timeStr == t }
          val clickData = ideaData2.filter { case (p_exp, slot1, idea1, feeType, ad1, clickCharge, displayCharge, pctr, timeStr) => p_exp == exp1 && slot1 == slot && ad1 == ad && timeStr == t }
          //val iosData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad && x._4 == 1}
          val d = displayData.count()
          val c = clickData.count()
          val ctr = c * 1.0 / d
          val p = displayData.map(x => x._8.toDouble).mean
          val dCharge = displayData.map(x => x._7.toDouble).sum()
          val cCharge = clickData.map(x => x._6.toDouble).sum()
          val cpm = (dCharge + cCharge) * 1.0 / d
          val cpc = (dCharge + cCharge) * 1.0 / c
          adsDebugBuf = adsDebugBuf :+ Array(d, c, ctr, p, cpm, cpc, dCharge, cCharge, t).mkString("\t")
        }
      }
    }
  }

  ideaData.unpersist()
  ideaDataRaw.unpersist()
}

println(adsDebugBuf.mkString("\n"))

/*
import java.util.Date
import java.text.SimpleDateFormat

val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
val dt = new Date()
val suffix = dateFormat.format(dt)

sc.parallelize(topAds).saveAsTextFile("training_pipeline/certainDisplayDebug/"+suffix+"/topAds")
sc.parallelize(topAdsCpc).saveAsTextFile("training_pipeline/certainDisplayDebug/"+suffix+"/topAdsCpc")
sc.parallelize(adsDebugBuf).saveAsTextFile("training_pipeline/certainDisplayDebug/"+suffix+"/adsDebugBuf")
*/