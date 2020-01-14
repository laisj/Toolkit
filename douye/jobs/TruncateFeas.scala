package Data

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.io.compress.GzipCodec

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-19
  */
object TruncateFeas {
  def main(args: Array[String]) {

    val Array(feaPath, validFeasPath, output_path) = args

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

    val par = new HashPartitioner(100)

    val feaMap = sc.textFile(validFeasPath).map(e => (e.trim, 1))
      .partitionBy(par)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val afterFilter = sc.textFile(feaPath)
        .map {
          case e=> {
            val segs = e.split("\t")
            var key = ""
            var deli = ""
            for (i <- 0 until segs.length-1) {
              key += deli + segs(i)
              deli = "\t"
            }
            (segs(segs.length-1), key)
          }
        }
      .partitionBy(par)
      .flatMap(e=>for (fea<-e._1.split(" ")) yield (fea.split(":")(0), e._2))
      .leftOuterJoin(feaMap, 1000)
      .map {
        case (fea, (info, count)) => {
          if (count.isDefined) {
            (info, fea + ":1")
          } else {
            null
          }
        }
      }
      .filter(_!=null)
      .reduceByKey(_+" "+_)
      .map(e=>e._1+"\t"+e._2)
      .saveAsTextFile(output_path, classOf[GzipCodec])

  }
}
