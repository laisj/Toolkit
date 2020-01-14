package Job

import com.xiaomi.miui.ad.predict.thrift.model._
import org.apache.commons.lang.StringUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.util

import com.xiaomi.data.commons.thrift.ThriftSerde
import com.xiaomi.miui.ad.predict.feature._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD


/**
  * Created: wwxu(xuwenwen@xiaomi.com)
  * Date: 2017-09-12
  */
object ContestFeatureGenerator {
  def checkRawData(str: String, colNum: Int) : Boolean = {
    if (StringUtils.isBlank(str) || str.split("\t").length != colNum) {
      return false
    }
    return true
  }

  def parseAdInfo(rawData:String): (Long, AdData) = {
    if (!checkRawData(rawData, 4)) {
      return null
    }
    val adData = new AdData();
    val segs = rawData.split("\t");
    adData.setAssetId(segs(0).toLong)  // 广告主id
    adData.setCampaignId(segs(1))   // campaign id
    adData.setId(segs(2).toLong)  // ad id
    val appId = segs(3).toLong // app id
    (appId, adData)
  }

  def parseAppInfo(rawData: String, termMap: scala.collection.Map[String, Long]): (Long, AppInfo) = {
    if (!checkRawData(rawData, 4)) {
      return null
    }
    val segs = rawData.split("\t")
    val appInfo = new AppInfo()
    appInfo.setAppId(segs(0).toLong)
    var discription = ""
    for (term <- segs(1).split(",")) {
      if(termMap.contains(term)) {
        discription += term + ","
      }
    }
    if (discription.length > 1) {
      appInfo.setDisplayName(discription)
    }
    appInfo.setLevel1CategoryId(segs(2).toInt)
    appInfo.setLevel2CategoryId(segs(3).toInt)
    (appInfo.getAppId, appInfo)
  }

  def parseUserInfo(rawData: String) : (String, UserData) = {
    if (!checkRawData(rawData, 8)) {
      return null
    }
    val segs = rawData.split("\t")
    val userData = new UserData()
    val uid = segs(0)
    userData.setImei(uid)
    val age = {
      try {
        Age.findByValue(segs(1).toInt%8)
      }
      catch {
        case e: Exception => Age.UNKNOWN
      }
    }
    userData.setAge(age)
    val gender = {
      try {
        Gender.findByValue(segs(2).toInt%3)
      }
      catch {
        case e: Exception => Gender.UNKNOWN
      }
    }
    userData.setGender(gender)
    userData.setDegreeStr(segs(3))
    userData.setProvinceCode(segs(4).toInt)
    userData.setCity(segs(5))
    userData.setDeviceInfo(segs(6).toInt)
    if (!segs(7).equals("\\N")) {
      val installedAppList = new util.ArrayList[java.lang.Long]()
      for (appId <- segs(7).split(",")) {
        installedAppList.add(appId.toLong)
      }
      val iApps = new IApps()
      iApps.setInstalledApps(installedAppList)
      userData.setIApps(iApps)
    }
    (uid, userData)
  }

  def parseEvent(line: String) : (String, (AdEvent, ContextData)) = {
    if(!checkRawData(line, 10)) {
      return null;
    }
    try {
      val adEvent = new AdEvent()
      val content = line.split("\t", 10)
      adEvent.setTriggerId(content(0))
      adEvent.setDownload(content(1))
      adEvent.setTimestamp(content(2))  // ddhhmm
      adEvent.setAdId(content(3))
      adEvent.setImei(content(4))
      adEvent.setTagId(content(5))
      val context = new ContextData()
      context.setTimestamp(adEvent.getTimestamp.toLong)  //ddhhmm
      context.setTagId(content(5))
      val env = new EnvironmentData()
      env.setConnectionType(content(6))
      env.setIp(content(8))
      context.setEnvironmentData(env)
      val deviceData = new DeviceData()
      deviceData.setAndroidVersion(content(7))
      deviceData.setMiuiVersion(content(9))
      context.setDeviceData(deviceData)
      (adEvent.getAdId, (adEvent, context))
    }
    catch {
      case e: Exception => null
    }
  }

  def parseAppActivationData(line: String): (String, String) = {
    if (!checkRawData(line, 4)) {
      return null
    }
    val segs = line.split("\t")
    val key = segs(0) + "_" + segs(1)
    val total = segs(2).toLong
    val activate = segs(3).toLong
    val rate = ((activate * 1.0 / total) * 1000).toInt.toString
    val group = Math.log10(total).toInt.toString
    val value = group + "_" + rate
    (key, value)
  }

  def generateFeature(item: (AdEvent, AdData, UserData, ContextData))
  : String = {
    val historicalDataCorpus = null
    val adEvent: AdEvent = item._1
    val adData: AdData = item._2
    val userData: UserData = item._3
    val contextData: ContextData = item._4
    val featureMap: java.util.Map[java.lang.String, java.lang.Double] = new java.util.HashMap[java.lang.String, java.lang.Double]()
    new AdInfoTagCT().get(userData, adData, contextData, historicalDataCorpus, featureMap)
    new UserInfoTagCT().get(userData, adData, contextData, historicalDataCorpus, featureMap)
    new ContextInfoTagCT().get(userData, adData, contextData, historicalDataCorpus, featureMap)
    new AXUTagCT().get(userData, adData, contextData, historicalDataCorpus, featureMap)
    dumpFeature(adEvent, featureMap)
  }

  def createUserDataByImei (imei : String): UserData ={
    val userData : UserData = new UserData
    userData.setAge(Age.UNKNOWN)
    userData.setGender(Gender.UNKNOWN)
    userData.setProvinceCode(-1)
    userData.setDegreeStr("\\N")
    userData.setCity("-1")
    userData.setDeviceInfo(-1)
    userData.setImei(imei)
    return userData
  }

  def createAdDataByAdId(id: String): AdData = {
    val adData: AdData = new AdData
    adData.setId(id.toLong)
    adData.setAssetId(-1)
    adData.setCampaignId("-1")   // campaign id
    adData
  }

  def dumpFeature(result: (AdEvent, java.util.Map[java.lang.String, java.lang.Double])): String = {
    val adEvent = result._1
    val featureMap = result._2
    val stringBuffer = new StringBuffer()
    var delim = ""
    val iter = featureMap.entrySet().iterator()
    while(iter.hasNext){
      val entry = iter.next
      val value = entry.getValue
      if(value == 0) {
      }
      if(value == 1.0)
        stringBuffer.append(delim + entry.getKey + ":1")
      else
        stringBuffer.append(delim + entry.getKey + ":" + value)
      delim = " "
    }
    adEvent.getDownload + "\t" + adEvent.getImei + "\t" + adEvent.getTriggerId + "\t" + adEvent.getAdId + "\t" +
      adEvent.getTagId + "\t" + adEvent.getTimestamp + "\t" + stringBuffer.toString
  }

  def main (args: Array[String]) {
    val Array(adInfoPath, appInfoPath, adAppActivatePath
    , userInfoPath, userAppUsagePath, userAppActionPath, userAdActivatePath, userQueryPath
    , adEventPath, outputfiles, flight) = args
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

    val termMap = sc.textFile(appInfoPath).flatMap(e=>for(term<-e.split("\t")(1).split(",")) yield (term, 1l))
      .reduceByKey(_+_)
      .filter(_._2 < 4000)
      .filter(_._2 > 3)
      .collectAsMap()
    ///user/h_data_platform/platform/miuiads/contest_dataset_app_category/data
    val appInfoData = sc.textFile(appInfoPath).map(parseAppInfo(_,termMap)).filter(_!=null).persist(StorageLevel.MEMORY_AND_DISK)

//    val adAppActivateDataMap = sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.BytesWritable](adAppActivatePath)
//      .map{
//        e=>{
//          val appActivateData = new AppActivateData()
//          ThriftSerde.deserialize(appActivateData, e._2.getBytes)
//          (e._1.toString, appActivateData)
//        }
//      }
//      .collectAsMap()

    ///user/h_data_platform/platform/miuiads/contest_dataset_ad/data
    val adInfoData = sc.textFile(adInfoPath).map(parseAdInfo(_)).filter(_!=null)
      .leftOuterJoin(appInfoData)
      .map {
        case (appId, (adData, appInfo)) => {
          if (appInfo.isDefined) {
            val info = appInfo.get
//            if (adAppActivateDataMap.contains(appId.toString)) {
//              info.setAppActivateData(adAppActivateDataMap.get(appId.toString).get)
//            }
            adData.setAppInfo(appInfo.get)
          }
          (adData.getId.toString, adData)
        }
      }
      .partitionBy(par)
      .persist(StorageLevel.MEMORY_AND_DISK)

//    val userActivateBehaviorData = sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.BytesWritable](userAdActivatePath)
//      .map {
//        e=>{
//          val activateBehavior = new ActivateBehavior()
//          ThriftSerde.deserialize(activateBehavior, e._2.getBytes)
//          (e._1.toString, activateBehavior)
//        }
//      }
//      .filter(_ != null)
//      .partitionBy(par).persist(StorageLevel.MEMORY_AND_DISK)

    val userAppUsageData = sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.BytesWritable](userAppUsagePath)
      .map{
        e => {
          val appUsageData = new AppUsageData()
          ThriftSerde.deserialize(appUsageData, e._2.getBytes)
          (e._1.toString, appUsageData)
        }
      }.partitionBy(par).persist(StorageLevel.MEMORY_AND_DISK)

    val userAppActionData = sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.BytesWritable](userAppActionPath)
      .map{
        e => {
          val appActionInfoData = new AppActionInfoData()
          ThriftSerde.deserialize(appActionInfoData, e._2.getBytes)
          (e._1.toString, appActionInfoData)
        }
      }.partitionBy(par).persist(StorageLevel.MEMORY_AND_DISK)


    val userQueryData = sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.BytesWritable](userQueryPath)
      .map{
        e => {
          val appQueryData = new AppQueryData()
          ThriftSerde.deserialize(appQueryData, e._2.getBytes)
          (e._1.toString, appQueryData)
        }
      }.partitionBy(par).persist(StorageLevel.MEMORY_AND_DISK)

    ///user/h_data_platform/platform/miuiads/contest_dataset_user_profile/data
    val userInfoData = sc.textFile(userInfoPath).map(parseUserInfo(_))
      .filter(_!=null).partitionBy(par)
      .leftOuterJoin(userAppUsageData, 100)
      .map{
        case (imei, (userData, appUsageData)) => {
          if (appUsageData.isDefined) {
            userData.setAppUsageData(appUsageData.get)
          }
          (imei, userData)
        }
      }
      .leftOuterJoin(userAppActionData, 100)
      .map{
        case (imei, (userData, appActionInfoData)) => {
          if (appActionInfoData.isDefined) {
            userData.setAppActionInfoData(appActionInfoData.get)
          }
          (imei, userData)
        }
      }
//      .leftOuterJoin(userActivateBehaviorData, 100)
//      .map{
//        case (imei, (userData, activateBehavior)) => {
//          if (activateBehavior.isDefined) {
//            userData.setActivateBehavior(activateBehavior.get)
//          }
//          (imei, userData)
//        }
//      }
      .leftOuterJoin(userQueryData, 100)
        .map {
          case (imei, (userData, queryData)) => {
            if (queryData.isDefined) {
              userData.setAppQueryData(queryData.get)
            }
            (imei, userData)
          }
        }
      .persist(StorageLevel.MEMORY_AND_DISK)

    ///user/h_data_platform/platform/miuiads/contest_dataset_label/data/date=11
    val adEventData = sc.textFile(adEventPath).map(parseEvent(_))
      .filter(_!=null)
      .partitionBy(par)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val joinData = adEventData.leftOuterJoin(adInfoData, 100)
      .map{
        case (adId, ((adEvent, contextData), adData)) => {
          if(adData.isDefined) {
            (adEvent.getImei(), (adEvent, adData.get, contextData))
          } else {
            (adEvent.getImei(), (adEvent, createAdDataByAdId(adId), contextData))
          }
        }
      }
      .leftOuterJoin(userInfoData, 100)
      .map{
        case (imei, ((adEvent, adData, contextData), userData)) => {
          if(userData.isDefined) {
            (adEvent, adData, userData.get , contextData)
          } else {
            (adEvent, adData, createUserDataByImei(imei), contextData)
          }
        }
      }
      .map(generateFeature(_))
      .coalesce(4000, false).saveAsTextFile(outputfiles + flight, classOf[GzipCodec])

  }

}
