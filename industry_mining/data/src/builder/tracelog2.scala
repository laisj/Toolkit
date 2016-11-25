package builder

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

// traceId|slotId|displayTime |prediction|feeType|displayCharge|expName|ctrModelName|userMomoId|displayCount|
// clickCharge|clickCount|queryTime |modelScore|feature

// batch pull data

object tracelog2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName))

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //val dt1 = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000)
    //val dt2 = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 2000)
    //val dt3 = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 3000)

    System.currentTimeMillis()

    val date2 = args(0)
    val dt2 = dateFormat.parse(date2)
    val dt1 = new Date(dt2.getTime() + 24 * 60 * 60 * 1000)
    val dt3 = new Date(dt2.getTime() - 24 * 60 * 60 * 1000)

    val date1 = dateFormat.format(dt1)
    val date3 = dateFormat.format(dt3)

    val featureData = sc.textFile("rtlog/"+date3+"/*/*/adengine/feature,rtlog/"+date2+"/*/*/adengine/feature")
    val displayData = sc.textFile("rtlog/"+date2+"/*/*/adengine/display")
    val clickData = sc.textFile("rtlog/"+date2+"/*/*/adengine/click,rtlog/"+date1+"/*/*/adengine/click")

    val featureArr = featureData.map{ x=>
      val xarr = x.split("\\|")
      val traceId = xarr(2)
      val slotId = xarr(3)
      val queryTime = xarr(0)
      val modelScore = xarr(6)
      val feature = xarr(5)
      ((traceId, slotId),(queryTime, modelScore, feature))
    }

    val displayArr = displayData.map{ x=>
      val xarr = x.split("\\|")
      val traceId = xarr(2)
      val slotId = xarr(10)
      val displayTime = xarr(0)
      val prediction = xarr(20)
      val feeType = xarr(12)
      val displayCharge = xarr(6)
      val expName = xarr(17)
      val strategy = xarr(18)
      val userMomoId = xarr(9)
      ((traceId, slotId), ((displayTime, prediction, feeType, displayCharge, expName, strategy, userMomoId),1))
    }.filter(x=>x._2._1._3 == "1").reduceByKey((x,y)=> (x._1, x._2+y._2))

    val clickArr = clickData.map{ x=>
      val xarr = x.split("\\|")
      val traceId = xarr(3)
      val slotId = xarr(10)
      val clickCharge = xarr(6)
      ((traceId, slotId), (clickCharge,1))
    }.reduceByKey((x,y)=> (x._1, x._2+y._2))

    val clickBuf = clickArr.distinct().collectAsMap()
    sc.broadcast(clickBuf)

    val ctrArr = displayArr.map{ x=>
      var clickCharge = "0.0"
      var label = 0
      if (clickBuf.contains(x._1)) {
        clickCharge = clickBuf(x._1)._1
        label = clickBuf(x._1)._2
      }
      (x._1, (x._2._1, x._2._2, clickCharge, label))
    }

    // join ctrArr and feature arr
    val parseArr = ctrArr.join(featureArr)
    val parseFlat = parseArr.map(x => Array[String](x._1._1, x._1._2, x._2._1._1._1, x._2._1._1._2, x._2._1._1._3,
      x._2._1._1._4, x._2._1._1._5, x._2._1._1._6,
      x._2._1._1._7, x._2._1._2.toString, x._2._1._3, x._2._1._4.toString,
      x._2._2._1, x._2._2._2, x._2._2._3))

    val extraction = parseFlat.flatMap { x =>
      var dupArr = Array[Array[String]]()
      val displayCnt = x(9).toInt
      val clickCnt = x(11).toInt
      if (clickCnt >= displayCnt || clickCnt == 0) {
        dupArr = Array.fill(displayCnt)(x)
      } else {
        dupArr = Array.fill(clickCnt)(x)
        val zeroLabel = x.take(11) ++ Array[String]("0") ++ x.takeRight(3)
        dupArr = dupArr ++ Array.fill(displayCnt-clickCnt)(zeroLabel)
      }
      dupArr
    }

    val parseStr = extraction.map {x => x.mkString("|")}

    parseStr.saveAsTextFile("parse/"+date2)

  }
}
