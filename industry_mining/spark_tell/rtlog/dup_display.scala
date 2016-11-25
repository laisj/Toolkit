import org.apache.spark._
val sc = new SparkContext(new SparkConf())

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val startDate: String = "2016-10-01"

val date: Date = dateFormat.parse(startDate)
val calendar: Calendar = Calendar.getInstance
calendar.setTime(date)
val ts = calendar.getTimeInMillis

var dateBuf = Array[Any]()
var displayBuf = Array[Any]()
var clickBuf = Array[Any]()
var displayUniqBuf = Array[Any]()
var clickUniqBuf = Array[Any]()
var clickFeeBuf = Array[Any]()

(0 to 1).foreach { x =>
  val dt = new Date(ts + 24.toLong * 60 * 60 * 1000 * x)
  val date = dateFormat.format(dt)
  val displayData = sc.textFile("rtlog" +date+"/*/*/adengine/display")
  val clickData = sc.textFile("rtlog" +date+"/*/*/adengine/click")

  val displayArr = displayData.map{ x=>
    val xarr = x.split("\\|")
    val traceId = xarr(2)
    val slotId = xarr(10)
    val feeType = xarr(12)
    val fee = xarr(6)
    (traceId, slotId, feeType, fee)
  }.filter(x=>x._3 == "1" && x._2 == "2-4").cache()

  val clickArr = clickData.map{ x=>
    val xarr = x.split("\\|")
    val traceId = xarr(3)
    val slotId = xarr(10)
    val feeType = xarr(12)
    val fee = xarr(6)
    (traceId, slotId, feeType, fee)
  }.filter(x=>x._3 == "1" && x._2 == "2-4").cache()

  val display = displayArr.count()
  val displayUniq = displayArr.map{x=>((x._1,x._2),1)}.reduceByKey(_+_).filter(x=>x._2==1).count()
  val click = clickArr.count()
  val clickWithFee = clickArr.filter(x=>x._4.toDouble > 0.0).count()
  val clickUniq = clickArr.map{x=>((x._1,x._2),1)}.reduceByKey(_+_).filter(x=>x._2==1).count()

  dateBuf = dateBuf :+ date
  displayBuf = displayBuf :+ display
  clickBuf = clickBuf :+ click
  displayUniqBuf = displayUniqBuf :+ displayUniq
  clickUniqBuf = clickUniqBuf :+ clickUniq
  clickFeeBuf = clickFeeBuf :+ clickWithFee

  displayArr.unpersist()
  clickArr.unpersist()
}
