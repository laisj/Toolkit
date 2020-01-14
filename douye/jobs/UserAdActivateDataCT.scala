package Data

import java.util

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import Util.Common._
import com.xiaomi.miui.ad.predict.thrift.model.{ActivateBehavior, ActivateStat}
import org.apache.spark.storage.StorageLevel
import com.xiaomi.data.commons.thrift.ThriftSerde
import org.apache.hadoop.io.compress.DefaultCodec

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-16
  */
object UserAdActivateDataCT {
  def parseActivationBehavior(imei: String, stat:(Long, Long), daySpan:Int) : (String, util.HashMap[java.lang.Integer, ActivateStat]) = {
    val activateStat = new ActivateStat()
    activateStat.setStartDownloadCount(stat._1)
    activateStat.setAppUsageCount(stat._2)
    val activateMap = new util.HashMap[java.lang.Integer, ActivateStat]()
    activateMap.put(daySpan, activateStat)
    (imei, activateMap)
  }

  def main (args: Array[String]) {
    val Array(eventPath, outputfiles, ds) = args
    val sparkConf = new SparkConf()
      .setAppName("AdInfoFeatureGeneration").set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "8").set("spark.cores.max", "32")
      .set("spark.akka.frameSize", "100").set("spark.shuffle.manager", "SORT")
      .set("spark.yarn.executor.memoryOverhead", "2047")
      .set("spark.yarn.driver.memoryOverhead", "896")
      .set("spark.memory.fraction", "0.3")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "Util.DataClassRegistrator")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }
    println("master: " + sparkConf.get("spark.master"))
    val sc = new SparkContext(sparkConf)

    val par = new HashPartitioner(100)

    val data1 = sc.textFile(genInputDirsStrCT(eventPath, ds, 1))
      .map(e=>(e.split("\t")(4), (1l, e.split("\t")(1).toLong)))
      .partitionBy(par)
      .reduceByKey((a, b) => (a._1+b._1, a._2+b._2))
      .map(e=>(parseActivationBehavior(e._1,e._2,1)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val data3 = sc.textFile(genInputDirsStrCT(eventPath, ds, 3))
      .map(e=>(e.split("\t")(4), (1l, e.split("\t")(1).toLong)))
      .partitionBy(par)
      .reduceByKey((a, b) => (a._1+b._1, a._2+b._2))
      .map(e=>(parseActivationBehavior(e._1,e._2,3)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val data7 = sc.textFile(genInputDirsStrCT(eventPath, ds, 7))
      .map(e=>(e.split("\t")(4), (1l, e.split("\t")(1).toLong)))
      .partitionBy(par)
      .reduceByKey((a, b) => (a._1+b._1, a._2+b._2))
      .map(e=>(parseActivationBehavior(e._1,e._2,7)))
      .leftOuterJoin(data3, 100)
      .map {
        case (imei, (map7, map3)) => {
          if (map3.isDefined && map3.get.containsKey(3)) {
            map7.put(3, map3.get.get(3))
          }
          (imei, map7)
        }
      }
      .leftOuterJoin(data1, 100)
      .map {
        case (imei, (map7, map1)) => {
          if (map1.isDefined && map1.get.containsKey(1)) {
            map7.put(1, map1.get.get(1))
          }
          val activateBehavior = new ActivateBehavior()
          activateBehavior.setActivateMap(map7)
          (imei, ThriftSerde.serialize(activateBehavior))
        }
      }
      .repartition(10)
      .saveAsSequenceFile(outputfiles, Some(classOf[DefaultCodec]))
      //.saveAsTextFile(outputfiles)
  }

}
