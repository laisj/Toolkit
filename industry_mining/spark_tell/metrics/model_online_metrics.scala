import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

// 7月12日 exp0 cpc广告 2-4上的auc mse oe

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.json4s._
import org.json4s.jackson.JsonMethods._

// val data = sc.textFile("tracelog/parse-/2016-07-11/,tracelog/parse-/2016-07-12/")

{
  val data = sc.textFile("tracelog/parse-/2016-10-12/").repartition(1000).cache()
  val darr = data.map(x => x.split("\\|")).filter(x => x(16) == "exp2" && x(8) == "1").map(x => (x(9).toDouble, x(1).toDouble))
  val auc = new BinaryClassificationMetrics(darr).areaUnderROC
  val mse = darr.map { case (p, v) => math.pow(v - p, 2) }.mean()
  val oe = darr.map(x => x._1).sum / darr.map(x => x._2).sum
  val neup = -darr.map { case (p, v) => v * math.log(p) + (1 - v) * math.log(1 - p) }.mean()
  val avgp = darr.map { case (p, v) => p }.mean()
  val nedown = -avgp * math.log(avgp) - (1 - avgp) * math.log(1 - avgp)
  val matric_ne = neup / nedown
  println(auc, mse, oe, matric_ne)
}

//2

{
  val data = sc.textFile("tracelog/parse-/2016-07-17/").repartition(1000).cache()
  var auc_map = collection.immutable.HashMap[String, Double]()
  var mse_map = collection.immutable.HashMap[String, Double]()
  var oe_map = collection.immutable.HashMap[String, Double]()
  for (pos <- Array("2-4", "2-12", "1-3", "1-10", "1-30")) {
    for (flight <- Array("exp5", "exp7", "exp0", "exp1")) {
      val darr = data.map(x => x.split("\\|")).filter(x => x(16) == flight && x(8) == "1" && x(10) == pos).map(x => (x(9).toDouble, x(1).toDouble))
      val auc = new BinaryClassificationMetrics(darr).areaUnderROC
      val mse = darr.map { case (p, v) => math.pow(v - p, 2) }.mean()
      val oe = darr.map(x => x._1).sum / darr.map(x => x._2).sum
      auc_map += ((flight + pos, auc))
      mse_map += ((flight + pos, mse))
      oe_map += ((flight + pos, oe))
    }
  }
  println(auc_map)
  println(mse_map)
  println(oe_map)
  oe_map.toSeq.sortBy(_._1).foreach(x => println(x._1 + "\t" + x._2))
}

// 3

{
  val data = sc.textFile("tracelog/parse-/2016-07-17/").repartition(1000).cache()
  var auc_map = collection.immutable.HashMap[String, Double]()
  var mse_map = collection.immutable.HashMap[String, Double]()
  var oe_map = collection.immutable.HashMap[String, Double]()
  var count_map = collection.immutable.HashMap[String, Double]()
  for (pos <- Array("2-4", "2-12", "1-3", "1-10", "1-30")) {
    for (flight <- Array("exp5", "exp7", "exp0", "exp1")) {
      val darr = data.map(x => x.split("\\|")).filter(x => x(16) == flight && x(8) == "1" && x(10) == pos)
      val darrclean = darr.filter { x =>
        implicit val formats = DefaultFormats
        x(15).length > 0 && (parse(x(15)) \ "posdisplay") != JNothing
      }
      count_map += ((pos + flight, darr.count))
      count_map += ((pos + flight + "clean", darrclean.count))
      val auc = new BinaryClassificationMetrics(darr.map(x => (x(9).toDouble, x(1).toDouble))).areaUnderROC
      val mse = darr.map(x => (x(9).toDouble, x(1).toDouble)).map { case (p, v) => math.pow(v - p, 2) }.mean()
      val oe = darr.map(x => (x(9).toDouble, x(1).toDouble)).map(x => x._1).sum / darr.map(x => (x(9).toDouble, x(1).toDouble)).map(x => x._2).sum
      auc_map += ((flight + pos, auc))
      mse_map += ((flight + pos, mse))
      oe_map += ((flight + pos, oe))
      for (dseg <- Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) {

        val segdata = darrclean.filter { x =>
          implicit val formats = DefaultFormats
          ((parse(x(15)) \ "posdisplay").extract[Double] / 1000).toInt == dseg
        }.map(x => (x(9).toDouble, x(1).toDouble))

        val sauc = new BinaryClassificationMetrics(segdata).areaUnderROC
        val smse = segdata.map { case (p, v) => math.pow(v - p, 2) }.mean()
        val soe = segdata.map(x => x._1).sum / segdata.map(x => x._2).sum

        auc_map += ((flight + pos + dseg.toString, sauc))
        mse_map += ((flight + pos + dseg.toString, smse))
        oe_map += ((flight + pos + dseg.toString, soe))
        count_map += ((pos + flight + dseg.toString, segdata.count()))
      }
    }
  }
  println(auc_map)
  println(mse_map)
  println(oe_map)
  println(count_map)
}
