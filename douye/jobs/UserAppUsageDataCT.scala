package Data

import java.util

import com.xiaomi.miui.ad.predict.thrift.model.{AppUsageData, AppUsageInfo, AppUsageInfoData}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import Util.Common._
import org.apache.commons.lang.StringUtils
import com.xiaomi.data.commons.thrift.ThriftSerde
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.storage.StorageLevel

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-14
  */
object UserAppUsageDataCT {
  def parseAppUsageInfo(imei:String, usage: String, daySpan: Int) : (String, util.HashMap[java.lang.Integer, util.List[AppUsageInfo]]) = {
    if (StringUtils.isBlank(usage) || StringUtils.isBlank(imei)) {
      return null
    }
    val appUsageMap = new util.HashMap[java.lang.Integer, util.List[AppUsageInfo]]()
    val appUsageList = new util.ArrayList[AppUsageInfo]()
    for (usageInfo <- usage.split("\\|")) {
      val detail = usageInfo.split(",")
      if (detail.length == 3) {
        val appUsageInfo = new AppUsageInfo()
        appUsageInfo.setAppId(detail(0).toLong)
        appUsageInfo.setUtime(detail(1).toLong)
        appUsageInfo.setOtime(detail(2).toLong)
        appUsageList.add(appUsageInfo)
      }
    }
    appUsageMap.put(daySpan, appUsageList)
    (imei, appUsageMap)
  }

  def main (args: Array[String]) {
    val Array(appUsagePath, outputfiles, ds) = args
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

    val usageData1 = sc.textFile(genInputDirsStrCT(appUsagePath, ds, 1))
      .map(e=>(e.split("\t")(0), e.split("\t")(1)))
      .partitionBy(par)
      .flatMap(e => for (rec <- e._2.split("\\|")) yield (e._1 +"\t" + rec.split(",")(0), (rec.split(",")(1).toLong, rec.split(",")(2).toLong)))
      .reduceByKey{
        case ((utime1, otime1),(utime2, otime2)) => {
          (utime1+utime2, otime1+otime2)
        }
      }
      .map(e=>(e._1.split("\t")(0), e._1.split("\t")(1) + "," + e._2._1.toString + "," + e._2._2.toString))
      .reduceByKey(_+"|"+_)
      .map(e=>parseAppUsageInfo(e._1, e._2, 1))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val usageData3 = sc.textFile(genInputDirsStrCT(appUsagePath, ds, 3))
      .map(e=>(e.split("\t")(0), e.split("\t")(1)))
      .partitionBy(par)
      .flatMap(e => for (rec <- e._2.split("\\|")) yield (e._1 +"\t" + rec.split(",")(0), (rec.split(",")(1).toLong, rec.split(",")(2).toLong)))
      .reduceByKey{
        case ((utime1, otime1),(utime2, otime2)) => {
          (utime1+utime2, otime1+otime2)
        }
      }
      .map(e=>(e._1.split("\t")(0), e._1.split("\t")(1) + "," + e._2._1.toString + "," + e._2._2.toString))
      .reduceByKey(_+"|"+_)
      .map(e=>parseAppUsageInfo(e._1, e._2, 3))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val usageData7 = sc.textFile(genInputDirsStrCT(appUsagePath, ds, 7))
      .map(e=>(e.split("\t")(0), e.split("\t")(1)))
      .partitionBy(par)
      .flatMap(e => for (rec <- e._2.split("\\|")) yield (e._1 +"\t" + rec.split(",")(0), (rec.split(",")(1).toLong, rec.split(",")(2).toLong)))
      .reduceByKey{
        case ((utime1, otime1),(utime2, otime2)) => {
          (utime1+utime2, otime1+otime2)
        }
      }
      .map(e=>(e._1.split("\t")(0), e._1.split("\t")(1) + "," + e._2._1.toString + "," + e._2._2.toString))
      .reduceByKey(_+"|"+_)
      .map(e=>parseAppUsageInfo(e._1, e._2, 7))
//      .persist(StorageLevel.MEMORY_AND_DISK)
//
//    val usageData15 = sc.textFile(genInputDirsStrCT(appUsagePath, ds, 15))
//      .map(e=>(e.split("\t")(0), e.split("\t")(1)))
//      .partitionBy(par)
//      .flatMap(e => for (rec <- e._2.split("\\|")) yield (e._1 +"\t" + rec.split(",")(0), (rec.split(",")(1).toLong, rec.split(",")(2).toLong)))
//      .reduceByKey{
//        case ((utime1, otime1),(utime2, otime2)) => {
//          (utime1+utime2, otime1+otime2)
//        }
//      }
//      .map(e=>(e._1.split("\t")(0), e._1.split("\t")(1) + "," + e._2._1.toString + "," + e._2._2.toString))
//      .reduceByKey(_+"|"+_)
//      .map(e=>parseAppUsageInfo(e._1, e._2, 15))
//      .leftOuterJoin(usageData7, 100)
//      .map {
//        case (imei, (usageMap15, usageMap7)) => {
//          if (usageMap7.isDefined) {
//            val map7 = usageMap7.get
//            if (map7.containsKey(7)) {
//              usageMap15.put(7, map7.get(7))
//            }
//          }
//          (imei, usageMap15)
//        }
//      }
      .leftOuterJoin(usageData3, 100)
      .map {
        case (imei, (usageMap15, usageMap3)) => {
          if (usageMap3.isDefined) {
            val map7 = usageMap3.get
            if (map7.containsKey(3)) {
              usageMap15.put(3, map7.get(3))
            }
          }
          (imei, usageMap15)
        }
      }
      .leftOuterJoin(usageData1, 100)
      .map {
        case (imei, (usageMap15, usageMap1)) => {
          if (usageMap1.isDefined) {
            val map7 = usageMap1.get
            if (map7.containsKey(1)) {
              usageMap15.put(1, map7.get(1))
            }
          }
          val appUsageData = new AppUsageData()
          appUsageData.setAppUsageMap(usageMap15)
          (imei, ThriftSerde.serialize(appUsageData))
        }
      }
      .repartition(10)
      .saveAsSequenceFile(outputfiles, Some(classOf[DefaultCodec]))
  }
}
