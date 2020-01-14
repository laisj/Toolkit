package Data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-19
  */
object FilterUselessFeas {
  def main(args: Array[String]) {
    val Array(
    feaPath,
    cutoff,
    cutoff1,
    output_path) = args

    val sparkConf = new SparkConf().setAppName("[CollectFeaId]")

    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[4]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    sparkConf.set("spark.kryo.registrator", "com.xiaomi.miui.ad.contest.ofmi.data.MyRegistrator")
    sparkConf.set("spark.broadcast.compress", "true")
    sparkConf.set("spark.core.connection.ack.wait.timeout", "600")

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }

    println("master: " + sparkConf.get("spark.master"))
    val sc = new SparkContext(sparkConf)

    val feaMap = sc.textFile(feaPath)
      .map(e => e.split("\t")(e.split("\t").length - 1))
      .flatMap(e => for (feaName <- e.split(" ")) yield (feaName.split(":")(0), 1l))
      .reduceByKey(_ + _)
//      .filter(_._2 > 40)
//      .map(e=>(e._2, e._1))
//      .sortByKey()
//      .coalesce(1)
//      .saveAsTextFile(output_path)
      .filter(_._2 > cutoff.toLong)
      .filter(_._2 < cutoff1.toLong)
      .repartition(10)
      .map(e => e._1)
      .saveAsTextFile(output_path)
  }
}
