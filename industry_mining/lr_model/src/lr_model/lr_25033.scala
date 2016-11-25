package lr_model

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer

object lr_25033 {
  val sexArray = Array[Integer](1, 2)
  val osArray = Array[Integer](1, 2)
  val wifiArray = Array[Integer](1, 2)
  val slotArray = Array[String]("2-4", "1-3", "2-12", "1-10", "1-30")
  val industryFirstArray = Array[String]("101", "102", "103", "104", "105", "106", "107", "108", "109", "110",
    "111", "112", "113", "114", "115", "116", "117", "118", "119", "120",
    "121", "122", "123", "124", "125", "126", "127", "128", "129", "999")
  val provinceArray = Array[String](
    "110000", "120000", "130000", "140000", "150000", "210000", "220000", "230000", "310000", "320000",
    "330000", "340000", "350000", "360000", "370000", "410000", "420000", "430000", "440000", "450000",
    "460000", "500000", "510000", "520000", "530000", "540000", "610000", "620000", "630000", "640000",
    "650000", "710000", "810000", "820000")

  var customMomoIdArray = Array[String]("36249550", "217880342", "403982989", "406172737", "396946681", "380939727",
    "369090197", "394410237", "389305592", "250021895", "364346974", "398326314", "356436416", "403540575",
    "374020160", "403492212", "116049071", "331233996", "367939496", "288263251", "399106425", "354255864",
    "378637435", "183792701", "292708473", "407739554", "408647450", "395678025", "391856424", "408814298",
    "381386769", "367772708", "75465945", "216858673", "346243220", "382051638", "344021584", "342215473",
    "387153299", "398878448", "302652745", "323148800", "376245899", "331579366", "399857816", "354673035",
    "109672266", "406112778", "376328771", "392943965", "398043751", "373530220", "274696467", "402519797",
    "399514665", "393681609", "393363934", "185541499", "353939545", "406699925", "87680588", "385387092",
    "53965444", "11380238", "325933905", "353389100", "353455776", "329711579", "37021790", "387231747",
    "398767340", "394337802", "382272150", "408181693", "27307771", "237962386", "355979733", "394198331",
    "385800201", "408688162", "396176428", "323769377", "402871277", "368331062", "399750453", "404846187",
    "234005312", "358021771", "144064350", "402527975", "36151091", "118841829", "405850474", "267040888",
    "393324820", "401566160", "332043379", "335369364", "286432366")

  val posDisplaySize = 20001
  val posCtrSize = 26
  val posDisplayUnit = 1000.0
  val posCtrUnit = 0.001

//  val posDisplaySize = 11
//  val posCtrSize = 251
//  val posDisplayUnit = 20000.0
//  val posCtrUnit = 0.0001

  val userDisplaySize = 21
  val userCtrSize = 51
  val userDisplayUnit = 50.0
  val userCtrUnit = 0.001

  val metric_fileName = "tracelog/parse-/2016-08-[1-2]*/,tracelog/parse-/2016-09-*/"
  val metric_testFileName = "tracelog/parse-/2016-10-10/"
  val model_feature = "lr33"

  def calculateCombFeatureWeightIdx(arr1_idx: Int, arr2_idx: Int, arr2_len: Int): Int = {
    if (arr1_idx == -1 || arr2_idx == -1) {
      return -1
    }
    arr1_idx * arr2_len + arr2_idx
  }

  def getDisplayCtrIdx(displaySize:Int, ctrSize:Int, displayUnit:Double,
                       ctrUnit:Double, display:Double, click:Double): (Int,Int) ={
    var ctrIdx = -1
    var displayIdx = -1
    if (display <= 0.0) {
      ctrIdx = ctrSize - 1
      displayIdx = displaySize - 1
    }
    else {
      displayIdx = (display / displayUnit).toInt
      if (displayIdx > displaySize - 2) {
        displayIdx = displaySize - 2
      }
      val ctr = click / display
      ctrIdx = (ctr / ctrUnit).toInt
      if (ctrIdx > ctrSize - 2) {
        ctrIdx = ctrSize - 2
      }
    }
    (displayIdx, ctrIdx)
  }

  def mkFeatureArr(idxArr: Array[Int], sizeArr: Array[Int]): Array[Int] = {
    var tempSize: Int = 0
    val len: Int = idxArr.length
    var featureArr: ArrayBuffer[Int] = ArrayBuffer[Int]()
    var i: Int = 0
    while (i < len) {
      featureArr += (idxArr(i) + tempSize)
      tempSize += sizeArr(i)
      i += 1
    }
    featureArr.toArray
  }

  def extractFeature(r: Array[String]): LabeledPoint = {
    implicit val formats = DefaultFormats

    val label = r(1).toDouble
    val posDisplay = (parse(r(15)) \ "posDisplay").extract[Double]
    val posClick = (parse(r(15)) \ "posClick").extract[Double]
    val userDisplay = (parse(r(15)) \ "userDisplay").extract[Double]
    val userClick = (parse(r(15)) \ "userClick").extract[Double]
    val slot = (parse(r(15)) \ "slotId").extract[String]
    val sex = (parse(r(15)) \ "sex").extract[Int]
    val industryFirstStr = (parse(r(15)) \ "industryFirst").extract[String]
    val os = (parse(r(15)) \ "os").extract[Int]
    val wifiType = (parse(r(15)) \ "wifiType").extract[Int]

    val age = (parse(r(15)) \ "age").extract[Int]
    val provCode = (parse(r(15)) \ "provCode").extract[String]
    val queryTime_hour = (parse(r(15)) \ "queryTime_hour").extract[Int]
    val customMomoId = (parse(r(15)) \ "customMomoId").extract[String]

    var slotIdx: Int = slotArray.indexOf(slot)
    if (slotIdx == -1) {
      slotIdx = slotArray.length
    }
    val slotSize = slotArray.length + 1

    var provIdx = provinceArray.indexOf(provCode)
    if (provIdx == -1) {
      provIdx = provinceArray.length
    }
    val provSize = provinceArray.length + 1

    val hourIdx = queryTime_hour
    val hourSize = 24

    var ageIdx = age
    if(age > 99 || age < 0) {
      ageIdx = 99
    }
    val ageSize = 100

    val posDisplayCtrPair = getDisplayCtrIdx(posDisplaySize, posCtrSize, posDisplayUnit, posCtrUnit, posDisplay,posClick)
    val posDisplayPosCtrIdx: Int = calculateCombFeatureWeightIdx(posDisplayCtrPair._1, posDisplayCtrPair._2, posCtrSize)
    val posDisplayPosCtrSize: Int = posDisplaySize * posCtrSize

    val userDisplayCtrPair = getDisplayCtrIdx(userDisplaySize, userCtrSize, userDisplayUnit, userCtrUnit, userDisplay,userClick)
    val userDisplayUserCtrIdx: Int = calculateCombFeatureWeightIdx(userDisplayCtrPair._1, userDisplayCtrPair._2, userCtrSize)
    val userDisplayUserCtrSize: Int = userDisplaySize * userCtrSize

    val sexIdx: Int = sexArray.indexOf(sex)
    var industryFirstIdx: Int = industryFirstArray.indexOf(industryFirstStr)
    if (industryFirstIdx == -1) {
      industryFirstIdx = industryFirstArray.length
    }

    var customMomoIdIdx: Int = customMomoIdArray.indexOf(customMomoId)
    if (customMomoIdIdx == -1) {
      customMomoIdIdx = customMomoIdArray.length
    }

    val guardCustomSlotSize: Int = (customMomoIdArray.length+1) * slotSize + 1
    var guardCustomSlotIdx: Int = guardCustomSlotSize -1
    if (posDisplayCtrPair._1 == posDisplaySize - 2) {
      guardCustomSlotIdx = calculateCombFeatureWeightIdx(customMomoIdIdx, slotIdx, slotSize)
    }

    val osIdx: Int = osArray.indexOf(os)
    val wifiIdx: Int = wifiArray.indexOf(wifiType)

    val sexIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(sexIdx, industryFirstIdx, industryFirstArray.length+1)
    val sexIndustryFirstSize: Int = sexArray.length * (industryFirstArray.length+1)

    val wifiIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(wifiIdx, industryFirstIdx, industryFirstArray.length+1)
    val wifiIndustryFirstSize: Int = wifiArray.length * (industryFirstArray.length+1)

    val osIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(osIdx, industryFirstIdx, industryFirstArray.length+1)
    val osIndustryFirstSize: Int = osArray.length * (industryFirstArray.length+1)



    val osSlotIdx: Int = calculateCombFeatureWeightIdx(osIdx, slotIdx, slotArray.length)
    val osSlotSize: Int = osArray.length * slotArray.length

    val wifiSlotIdx: Int = calculateCombFeatureWeightIdx(wifiIdx, slotIdx, slotArray.length)
    val wifiSlotSize: Int = wifiArray.length * slotArray.length

    val sexSlotIdx: Int = calculateCombFeatureWeightIdx(sexIdx, slotIdx, slotArray.length)
    val sexSlotSize: Int = sexArray.length * slotArray.length

    val ageSlotIdx: Int = calculateCombFeatureWeightIdx(ageIdx, slotIdx, slotArray.length)
    val ageSlotSize: Int = ageSize * slotArray.length

    val provSlotIdx: Int = calculateCombFeatureWeightIdx(provIdx, slotIdx, slotArray.length)
    val provSlotSize: Int = provSize * slotArray.length

    val hourSlotIdx: Int = calculateCombFeatureWeightIdx(hourIdx, slotIdx, slotArray.length)
    val hourSlotSize: Int = hourSize * slotArray.length

//    val features = mkFeatureArr(Array(posDisplayPosCtrIdx, userDisplayUserCtrIdx, sexIndustryFirstIdx, slotIdx,
//      osSlotIdx, wifiSlotIdx, sexSlotIdx, ageSlotIdx, provSlotIdx, wifiIndustryFirstIdx, osIndustryFirstIdx),
//      Array(posDisplayPosCtrSize, userDisplayUserCtrSize, sexIndustryFirstSize, slotArray.length, osSlotSize,
//        wifiSlotSize, sexSlotSize, ageSlotSize, provSlotSize, wifiIndustryFirstSize, osIndustryFirstSize))
//    val featureLen = posDisplayPosCtrSize + userDisplayUserCtrSize + sexIndustryFirstSize + slotArray.length +
//      osSlotSize + wifiSlotSize + sexSlotSize + ageSlotSize + provSlotSize + wifiIndustryFirstSize + osIndustryFirstSize

    val features = mkFeatureArr(Array(posDisplayPosCtrIdx, userDisplayUserCtrIdx, sexIndustryFirstIdx, slotIdx,
      osSlotIdx, wifiSlotIdx),
      Array(posDisplayPosCtrSize, userDisplayUserCtrSize, sexIndustryFirstSize, slotArray.length, osSlotSize,
        wifiSlotSize))
    val featureLen = posDisplayPosCtrSize + userDisplayUserCtrSize + sexIndustryFirstSize + slotArray.length +
      osSlotSize + wifiSlotSize

    LabeledPoint(label, Vectors.sparse(featureLen, features, Array(1, 1, 1, 1, 1, 1)))
  }

  def mkDebugBuf(predictionsAndLabels: RDD[(Array[String], (Double, Double))]): Array[String] = {
    predictionsAndLabels.cache()
    val metric_trainAuc = new BinaryClassificationMetrics(predictionsAndLabels.map(_._2)).areaUnderROC.toString
    val metric_trainMse = predictionsAndLabels.map { x => (x._2._1 - x._2._2) * (x._2._1 - x._2._2)}.mean().toString
    val metric_trainOe = predictionsAndLabels.map { x => x._2._1 - x._2._2}.sum().toString

    val oe1 = predictionsAndLabels.map(x => (x._1(0), (x._2._1, x._2._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)).sortBy(-_._2._3).collect().mkString("\t")
    val oe2 = predictionsAndLabels.map(x => (x._1(1), (x._2._1, x._2._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)).sortBy(-_._2._3).collect().mkString("\t")
    val oe3 = predictionsAndLabels.map(x => (x._1(2), (x._2._1, x._2._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3)).sortBy(-_._2._3).collect().mkString("\t")

    predictionsAndLabels.unpersist()

    Array("auc", metric_trainAuc, "mse", metric_trainMse, "oe", metric_trainOe, "oe1", oe1,
      "oe2", oe2, "oe3", oe3)
  }

  def main(args: Array[String]) {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = new Date()
    val suffix = dateFormat.format(dt)
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName + suffix))
    // train

    val rawTrainingData = sc.textFile(metric_fileName).sample(false, 0.20, System.currentTimeMillis().toInt)
    val recordsTrain = rawTrainingData.map(line => line.split("\\|"))

    val trainingData = recordsTrain.filter(x => x.length == 20 && x(8) == "1" && x(15).takeRight(1)=="}").map {extractFeature}

    trainingData.cache()
    val metric_trainCount = trainingData.count().toString

    val algorithm = new LogisticRegressionWithLBFGS()
    algorithm.setIntercept(true)

    val model = algorithm.run(trainingData)
    model.clearThreshold()

    val metrics_modelFeatureCnt = model.numFeatures.toString
    val metrics_modelWeights = model.weights.toArray.mkString("\t")
    val metrics_modelIntercept = model.intercept.toString

    val modelName = suffix + model_feature
    val modelPath = "training_pipeline/model/" + modelName

    // training auc
    val pl = trainingData.sample(false, 0.01, System.currentTimeMillis().toInt).map { point =>
      val prediction = model.predict(point.features)
      val index = point.features.toString.split("\\[|\\]")(1).split(",")
      (index, (prediction, point.label))
    }

    val debugTrainBuf = mkDebugBuf(pl)
    trainingData.unpersist()

    // test

    val rawTestData = sc.textFile(metric_testFileName)
    val recordsTest = rawTestData.map(line => line.split("\\|"))
    val testData = recordsTest.filter(x => x.length == 20 && x(8) == "1" && x(15).takeRight(1)=="}").map {extractFeature}

    testData.cache()
    val metric_testCount = testData.count().toString

    val testpl = testData.map { point =>
      val prediction = model.predict(point.features)
      val index = point.features.toString.split("\\[|\\]")(1).split(",")
      (index, (prediction, point.label))
    }

    val debugTestBuf = mkDebugBuf(testpl)
    testData.unpersist()

    var debugBuf = Array("train:", metric_fileName, metric_trainCount, metrics_modelFeatureCnt, metrics_modelWeights,
      metrics_modelIntercept, "test:", metric_testFileName, metric_testCount, "trainingData:")

    debugBuf = debugBuf ++ debugTrainBuf
    debugBuf = debugBuf :+ "testData:"
    debugBuf = debugBuf ++ debugTestBuf

    model.save(sc, modelPath)
    sc.parallelize(debugBuf,1).saveAsTextFile("training_pipeline/model_metadata/"+modelName)
  }
}
