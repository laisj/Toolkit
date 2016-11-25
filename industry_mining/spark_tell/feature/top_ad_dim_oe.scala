import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.Date
import java.text.SimpleDateFormat

val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
val dt = new Date()
val suffix = dateFormat.format(dt)

val dt_yesterday = new Date(System.currentTimeMillis()-24*60*60*2000)
val fileArr = Array("tracelog/parse-/" + new SimpleDateFormat("yyyy-MM-dd").format(dt_yesterday) + "/")

val expArr = Array("default")
val slotArr = Array("2-4","2-12","1-3","1-10","1-30")

// head ad on top
var topAds = Array[String]()
var topAdsCpc = Array[String]()
var adMap = Map[String,Array[(String,String)]]()

for (file <- fileArr) {
  topAds = topAds :+ file
  topAdsCpc = topAdsCpc :+ file
  val parseData = sc.textFile(file)
  val ideaData = parseData.map{ x=>
    implicit val formats = DefaultFormats
    val exp = x.split("\\|")(16)
    val slot = x.split("\\|")(10)
    val idea = (parse(x.split("\\|")(15)) \ "ideaId").extract[String]
    val ad = (parse(x.split("\\|")(15)) \ "adId").extract[String]
    val feeType = (parse(x.split("\\|")(15)) \ "feeType_cpc").extract[String]
    (exp,slot,idea,feeType,ad)
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
      adMap += (file + "_" + exp + "_" + slot -> ideaFilteredData.filter(x=>x._1._2=="1").map(x=>(x._1._1,x._1._3)))
    }
  }
  ideaData.unpersist()
}

// cpc accurracy
var adsDebugBuf = Array[String]()

for (file <- fileArr) {
  adsDebugBuf = adsDebugBuf :+ file
  val parseData = sc.textFile(file)
  val ideaDataRaw = parseData.map{x=>
    implicit val formats = DefaultFormats
    val exp = x.split("\\|")(16)
    val slot = x.split("\\|")(10)
    val idea = (parse(x.split("\\|")(15)) \ "ideaId").extract[String]
    val os = (parse(x.split("\\|")(15)) \ "os").extract[Double]
    val sex = (parse(x.split("\\|")(15)) \ "sex").extract[Double]
    val wifiType = (parse(x.split("\\|")(15)) \ "wifiType").extract[Double]
    val label = x.split("\\|")(1)
    val prediction = x.split("\\|")(9)
    (exp,slot,idea,os,sex,wifiType,label,prediction)
  }
  ideaDataRaw.cache()
  val ideaData = ideaDataRaw.filter(x=>x._8 != "-")
  ideaData.cache()
  adsDebugBuf = adsDebugBuf :+ ("raw:" + ideaDataRaw.count().toString + ",filtered:" + ideaData.count().toString)

  for (exp <- expArr) {
    adsDebugBuf = adsDebugBuf :+ exp
    for (slot <- slotArr) {
      adsDebugBuf = adsDebugBuf :+ slot
      for (ad <- adMap(file + "_" + exp + "_" + slot)) {
        adsDebugBuf = adsDebugBuf :+ (ad._1 + "_" + ad._2)
        // ios adr
        val iosData = ideaData.filter { case(exp1,slot1,idea1,os,sex,wifiType,label,prediction) => exp1 == exp && slot1 == slot && idea1 == ad._1 && os == 1}
        //val iosData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad && x._4 == 1}
        val iosd = iosData.count()
        val iosc = iosData.filter{x=>x._7=="1"}.count()
        val iosCtr = iosc * 1.0 / iosd
        val iosp = iosData.map(x => x._8.toDouble).mean
        adsDebugBuf = adsDebugBuf :+ Array("ios",iosd,iosc,iosCtr,iosp).mkString("\t")

        val adrData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad._1 && x._4 == 2}
        val adrd = adrData.count()
        val adrc = adrData.filter{x=>x._7=="1"}.count()
        val adrCtr = adrc * 1.0 / adrd
        val adrp = adrData.map(x => x._8.toDouble).mean
        adsDebugBuf = adsDebugBuf :+ Array("adr",adrd,adrc,adrCtr,adrp).mkString("\t")
        // male female
        val maleData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad._1 && x._5 == 1}
        val maled = maleData.count()
        val malec = maleData.filter{x=>x._7=="1"}.count()
        val maleCtr = malec * 1.0 / maled
        val malep = maleData.map(x => x._8.toDouble).mean
        adsDebugBuf = adsDebugBuf :+ Array("male",maled,malec,maleCtr,malep).mkString("\t")

        val femaleData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad._1 && x._5 == 2}
        val femaled = femaleData.count()
        val femalec = femaleData.filter{x=>x._7=="1"}.count()
        val femaleCtr = femalec * 1.0 / femaled
        val femalep = femaleData.map(x => x._8.toDouble).mean
        adsDebugBuf = adsDebugBuf :+ Array("female",femaled,femalec,femaleCtr,femalep).mkString("\t")
        // wifi?
        val wifiData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad._1 && x._6 == 1}
        val wifid = wifiData.count()
        val wific = wifiData.filter{x=>x._7=="1"}.count()
        val wifiCtr = wific * 1.0 / wifid
        val wifip = wifiData.map(x => x._8.toDouble).mean
        adsDebugBuf = adsDebugBuf :+ Array("wifi",wifid,wific,wifiCtr,wifip).mkString("\t")

        val fengwoData = ideaData.filter { x => x._1 == exp && x._2 == slot && x._3 == ad._1 && x._6 == 2}
        val fengwod = fengwoData.count()
        val fengwoc = fengwoData.filter{x=>x._7=="1"}.count()
        val fengwoCtr = fengwoc * 1.0 / fengwod
        val fengwop = fengwoData.map(x => x._8.toDouble).mean
        adsDebugBuf = adsDebugBuf :+ Array("fengwo",fengwod,fengwoc,fengwoCtr,fengwop).mkString("\t")
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

sc.parallelize(topAds).saveAsTextFile("training_pipeline/debug/"+suffix+"/topAds")
sc.parallelize(topAdsCpc).saveAsTextFile("training_pipeline/debug/"+suffix+"/topAdsCpc")
sc.parallelize(adsDebugBuf).saveAsTextFile("training_pipeline/debug/"+suffix+"/adsDebugBuf")
