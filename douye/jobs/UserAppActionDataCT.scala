package Data

import Util.Common.genInputDirsStrCT
import org.apache.commons.lang.StringUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import Util.Common._
import com.xiaomi.miui.ad.predict.thrift.model.{AppActionInfo, AppActionInfoData}
import java.util

import com.xiaomi.data.commons.thrift.ThriftSerde
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.storage.StorageLevel
/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-15
  */
object UserAppActionDataCT {
  def checkRawData(str: String, colNum: Int) : Boolean = {
    if (StringUtils.isBlank(str) || str.split("\t").length != colNum) {
      return false
    }
    return true
  }

  def parseActionRecord(str:String):(String, Long) = {
    if (!checkRawData(str, 4)) {
      return null
    }
    val segs = str.split("\t")
    (segs(0) + CTRL_A + segs(1) + CTRL_A + segs(2), 1l)
  }

  def genAppActionInfoData(imei: String, actions: String, daySpan:Int) : (String, util.HashMap[java.lang.Integer, util.List[AppActionInfo]]) = {
    val appActionMap = new util.HashMap[java.lang.Integer, util.List[AppActionInfo]]()
    val list = new util.ArrayList[AppActionInfo]()
    for (seg <- actions.split(CTRL_D)) {
      val appId = seg.split(CTRL_C)(0)
      for (stat <- seg.split(CTRL_C)(1).split(CTRL_B)) {
        val appActionInfo = new AppActionInfo()
        appActionInfo.setAppId(appId)
        appActionInfo.setActionType(stat.split(CTRL_A)(0))
        appActionInfo.setCounts(stat.split(CTRL_A)(1).toLong)
        list.add(appActionInfo)
      }
    }
    appActionMap.put(daySpan, list)
    (imei, appActionMap)
  }

  def main (args: Array[String]) {
    val Array(appActionPath, outputfiles, ds) = args
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


    val actionData1 = sc.textFile(genInputDirsStrCT(appActionPath, ds, 1))
      .map(parseActionRecord(_))
      .filter(_!=null)
      .partitionBy(par)
      .reduceByKey(_+_)
      .map(e=>(e._1.split(CTRL_A)(0)+CTRL_A+e._1.split(CTRL_A)(1), e._1.split(CTRL_A)(2) + CTRL_A + e._2.toString))
      .reduceByKey(_+CTRL_B+_)
      .map(e=>(e._1.split(CTRL_A)(0), e._1.split(CTRL_A)(1) + CTRL_C + e._2))
      .reduceByKey(_+CTRL_D+_)
      .map(e=>genAppActionInfoData(e._1, e._2, 1))
      .persist(StorageLevel.MEMORY_AND_DISK)


    val actionData3 = sc.textFile(genInputDirsStrCT(appActionPath, ds, 3))
      .map(parseActionRecord(_))
      .filter(_!=null)
      .partitionBy(par)
      .reduceByKey(_+_)
      .map(e=>(e._1.split(CTRL_A)(0)+CTRL_A+e._1.split(CTRL_A)(1), e._1.split(CTRL_A)(2) + CTRL_A + e._2.toString))
      .reduceByKey(_+CTRL_B+_)
      .map(e=>(e._1.split(CTRL_A)(0), e._1.split(CTRL_A)(1) + CTRL_C + e._2))
      .reduceByKey(_+CTRL_D+_)
      .map(e=>genAppActionInfoData(e._1, e._2, 3))
      .persist(StorageLevel.MEMORY_AND_DISK)


    val actionData7 = sc.textFile(genInputDirsStrCT(appActionPath, ds, 7))
      .map(parseActionRecord(_))
      .filter(_!=null)
      .partitionBy(par)
      .reduceByKey(_+_)
      .map(e=>(e._1.split(CTRL_A)(0)+CTRL_A+e._1.split(CTRL_A)(1), e._1.split(CTRL_A)(2) + CTRL_A + e._2.toString))
      .reduceByKey(_+CTRL_B+_)
      .map(e=>(e._1.split(CTRL_A)(0), e._1.split(CTRL_A)(1) + CTRL_C + e._2))
      .reduceByKey(_+CTRL_D+_)
      .map(e=>genAppActionInfoData(e._1, e._2, 7))
//      .persist(StorageLevel.MEMORY_AND_DISK)
//
//    val actionDat15 = sc.textFile(genInputDirsStrCT(appActionPath, ds, 15))
//      .map(parseActionRecord(_))
//      .filter(_!=null)
//      .partitionBy(par)
//      .reduceByKey(_+_)
//      .map(e=>(e._1.split(CTRL_A)(0)+CTRL_A+e._1.split(CTRL_A)(1), e._1.split(CTRL_A)(2) + CTRL_A + e._2.toString))
//      .reduceByKey(_+CTRL_B+_)
//      .map(e=>(e._1.split(CTRL_A)(0), e._1.split(CTRL_A)(1) + CTRL_C + e._2))
//      .reduceByKey(_+CTRL_D+_)
//      .map(e=>genAppActionInfoData(e._1, e._2, 15))
//      .leftOuterJoin(actionData7, 100)
//      .map{
//        case (imei, (appActionMap15, appActionMap7)) => {
//          if (appActionMap7.isDefined) {
//            val map7 = appActionMap7.get
//            if (map7.containsKey(7)) {
//              appActionMap15.put(7, map7.get(7))
//            }
//          }
//          (imei, appActionMap15)
//        }
//      }
      .leftOuterJoin(actionData3, 100)
      .map{
        case (imei, (appActionMap15, appActionMap3)) => {
          if (appActionMap3.isDefined) {
            val map7 = appActionMap3.get
            if (map7.containsKey(3)) {
              appActionMap15.put(3, map7.get(3))
            }
          }
          (imei, appActionMap15)
        }
      }
      .leftOuterJoin(actionData1, 100)
      .map{
        case (imei, (appActionMap15, appActionMap1)) => {
          if (appActionMap1.isDefined) {
            val map7 = appActionMap1.get
            if (map7.containsKey(1)) {
              appActionMap15.put(1, map7.get(1))
            }
          }

          val appActionInfoData = new AppActionInfoData()
          appActionInfoData.setAppActionMap(appActionMap15)
          (imei, ThriftSerde.serialize(appActionInfoData))
        }
      }
      .repartition(10)
      .saveAsSequenceFile(outputfiles, Some(classOf[DefaultCodec]))
  }
}
