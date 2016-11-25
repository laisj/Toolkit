package debug


import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by laisijia on 16/9/29.
  */
object low_ctr_oe {
  def main(args: Array[String]) {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val dt = new Date()
    val suffix = dateFormat.format(dt)

    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName + suffix))

    // day before yesterday
    val dt_yesterday = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 2000)

    //val fileArr = Array("tracelog/parse-/2016-08-19/","tracelog/parse-/2016-08-20/","tracelog/parse-/2016-08-21/")
    val fileArr = Array("tracelog/parse-/" + new SimpleDateFormat("yyyy-MM-dd").format(dt_yesterday) + "/")
    val expArr = Array("default")
    val slotArr = Array("2-4", "2-12", "1-3", "1-10", "1-30")

    // dim oe
    var topDimCpc = Array[String]()

    for (file <- fileArr) {
      topDimCpc = topDimCpc :+ file
      val parseData = sc.textFile(file).map(line => line.split("\\|")).
        filter(x => x.length == 20 && x(8) == "1" && x(9) != "-" && x(15).takeRight(1)=="}")
      val dimData2 = parseData.map { x =>
        implicit val formats = DefaultFormats
        val exp = x(16)
        val slot = x(10)
        val feeType = (parse(x(15)) \ "feeType_cpc").extract[String]
        val posDisplay = (parse(x(15)) \ "posDisplay2").extract[Double]
        val posClick = (parse(x(15)) \ "posClick2").extract[Double]
        val userDisplay = (parse(x(15)) \ "userDisplay").extract[Double]
        val userClick = (parse(x(15)) \ "userClick").extract[Double]
        val label = x(1)
        val prediction = x(9)
        (exp, slot, userDisplay, userClick, feeType, posDisplay, posClick, label, prediction)
      }.filter(x => x._9 != "-" && x._5 == "1")

      dimData2.cache()
      for (exp <- expArr) {
        topDimCpc = topDimCpc :+ exp
        for (slot <- slotArr) {
          topDimCpc = topDimCpc :+ slot

          val dimData = dimData2.filter(x => x._1 == exp && x._2 == slot)

          var curDimData = dimData.filter(x => x._3 < 1000)
          var d = curDimData.count()
          var c = curDimData.filter { x => x._8 == "1" }.count()
          var ctr = c * 1.0 / d
          var p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("low_user", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._4 / x._3 < 0.002)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("low_uctr", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 < 1000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("low_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 < 2000 && x._6 > 1000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("2000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 < 4000 && x._6 > 2000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("4000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 < 8000 && x._6 > 4000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("8000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 < 16000 && x._6 > 8000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("16000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 16000 && x._6 < 32000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("32000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 32000 && x._6 < 64000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("64000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 64000 && x._6 < 128000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("128000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 128000 && x._6 < 256000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("256000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 256000 && x._6 < 512000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("512000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 512000 && x._6 < 1024000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("1024000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 1024000 && x._6 < 2048000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("2048000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 2048000 && x._6 < 4096000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("4096000_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 > 4096000)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("high_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._7 / x._6 < 0.002)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("low_pctr", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._6 == 0.0)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("0_pdisplay", d, c, ctr, p).mkString("\t")

          curDimData = dimData.filter(x => x._3 == 0.0)
          d = curDimData.count()
          c = curDimData.filter { x => x._8 == "1" }.count()
          ctr = c * 1.0 / d
          p = curDimData.map(x => x._9.toDouble).mean
          topDimCpc = topDimCpc :+ Array("0_udisplay", d, c, ctr, p).mkString("\t")

        }
      }
      dimData2.unpersist()
    }

    println("topDimCpc\n")
    println(topDimCpc.mkString("\n"))

    sc.parallelize(topDimCpc, 1).saveAsTextFile("training_pipeline/dimDebug/" + suffix + "/topAdsCpc")
  }
}
