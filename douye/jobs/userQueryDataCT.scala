package Data

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import Util.Common._
import java.util

import com.xiaomi.miui.ad.predict.thrift.model.AppQueryData
import com.xiaomi.data.commons.thrift.ThriftSerde
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.storage.StorageLevel

/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-21
  */
object userQueryDataCT {
  val WIN = Array("1", "3", "7", "15")
  def getSpan(ds:String, ts:String):String = {
    val dsday = ds.substring(0, 2).toInt
    val tsday = ts.substring(0, 2).toInt
    val span = dsday - tsday + 1
    if (span == 1) {
      return "1"
    } else if (span > 1 && span <=3) {
      return "3"
    } else if (span >3 && span <= 7) {
      return "7"
    } else if (span > 7) {
      return "15"
    } else {
      return "0"
    }
  }

  def main (args: Array[String]) {
    val Array(userQueryPath, appInfoPath, outputfiles, ds, daySpan) = args
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

    val par = new HashPartitioner(1000)

    val appid2c1 = sc.textFile(appInfoPath)
      .map(e => (e.split("\t")(0), e.split("\t")(2)))
      .collectAsMap()

    val appid2c2 = sc.textFile(appInfoPath)
      .map(e => (e.split("\t")(0), e.split("\t")(3)))
      .collectAsMap()

    val data1 = sc.textFile(genInputDirsStrCT(userQueryPath, ds, daySpan.toInt))
      .flatMap {
        case e=> {
          val segs = e.split("\t")
          val ts = segs(0)
          val imei = segs(1)
          val appids = segs(2)
          val span = getSpan(ds, ts)
          val idx = WIN.indexOf(span)
          for (i <- 0 until idx+1)
            yield (imei + "\t" + WIN(i), appids)
        }
      }
      .reduceByKey(_+","+_)
      .map(e=>(e._1.split("\t")(0), e._1.split("\t")(1) + CTRL_A + e._2))
      .reduceByKey(_+CTRL_B+_)
      .map{
        case (imei, infos) => {
          val appQueryData = new AppQueryData()
          val appQueryMap = new util.HashMap[java.lang.Integer, util.List[java.lang.String]]()
          val c2QueryMap = new util.HashMap[java.lang.Integer, util.List[java.lang.String]]()
          val c1QueryMap = new util.HashMap[java.lang.Integer, util.List[java.lang.String]]()

          for (info <- infos.split(CTRL_B)) {
            val span = info.split(CTRL_A)(0)
            val appids = info.split(CTRL_A)(1)
            val applist = new util.ArrayList[java.lang.String]()
            val c1list = new util.ArrayList[java.lang.String]()
            val c2list = new util.ArrayList[java.lang.String]()
            val appset = new util.HashSet[String]()
            val c1set = new util.HashSet[String]()
            val c2set = new util.HashSet[String]()
            for (appid <- appids.split(",")) {
              if (!appset.contains(appid) && applist.size() <= 80) {
                applist.add(appid)
                appset.add(appid)
                if (appid2c1.contains(appid)) {
                  val c1 = appid2c1.get(appid).get
                  if (!c1set.contains(c1) && c1list.size() <= 40) {
                    c1list.add(c1)
                    c1set.add(c1)
                  }
                }
                if (appid2c2.contains(appid)) {
                  val c2 = appid2c2.get(appid).get
                  if (!c2set.contains(c2) && c2list.size() <= 60) {
                    c2list.add(c2)
                    c2set.add(c2)
                  }
                }
              }
            }
            if (!applist.isEmpty)
              appQueryMap.put(span.toInt, applist)
            if (!c1list.isEmpty)
              c1QueryMap.put(span.toInt, c1list)
            if (!c2list.isEmpty)
              c2QueryMap.put(span.toInt, c2list)
          }

          appQueryData.setAppQueryMap(appQueryMap)
          appQueryData.setC1QueryMap(c1QueryMap)
          appQueryData.setC2QueryMap(c2QueryMap)
          //(imei, appQueryData)
          (imei, ThriftSerde.serialize(appQueryData))
        }
      }
    //  .persist(StorageLevel.MEMORY_AND_DISK)
      .saveAsSequenceFile(outputfiles, Some(classOf[DefaultCodec]))
      //.saveAsTextFile(outputfiles)



  }
}
