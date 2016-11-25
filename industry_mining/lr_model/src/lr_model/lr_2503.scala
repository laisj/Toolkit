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

object lr_2503 {
  val sexArray = Array[Int](1, 2)
  val osArray = Array[Int](1, 2)
  val wifiArray = Array[Int](1, 2)
  val slotArray = Array[String]("2-4", "1-3", "2-12", "1-10", "1-30")
  val industryFirstArray = Array[String]("101", "102", "103", "104", "105", "106", "107", "108", "109", "110",
    "111", "112", "113", "114", "115", "116", "117", "118", "119", "120",
    "121", "122", "123", "124", "125", "126", "127", "128", "129", "999")
  val provinceArray = Array[String](
    "110000", "120000", "130000", "140000", "150000", "210000", "220000", "230000", "310000", "320000",
    "330000", "340000", "350000", "360000", "370000", "410000", "420000", "430000", "440000", "450000",
    "460000", "500000", "510000", "520000", "530000", "540000", "610000", "620000", "630000", "640000",
    "650000", "710000", "810000", "820000")

  val sexSize = sexArray.length + 1
  val osSize = osArray.length + 1
  val wifiSize = wifiArray.length + 1
  val slotSize = slotArray.length + 1
  val industryFirstSize = industryFirstArray.length + 1
  val provSize = provinceArray.length + 1
  val hourSize = 24
  val ageSize = 100

  val posDisplaySize = 3
  val posCtrSize = 51
  val posDisplayUnit = 1000.0
  val posCtrUnit = 0.001

  //  val posDisplaySize = 21
  //  val posCtrSize = 251
  //  val posDisplayUnit = 10000.0
  //  val posCtrUnit = 0.0001

  val userDisplaySize = 21
  val userCtrSize = 51
  val userDisplayUnit = 50.0
  val userCtrUnit = 0.001

  val posDisplayPosCtrSize: Int = posDisplaySize * posCtrSize
  val userDisplayUserCtrSize: Int = userDisplaySize * userCtrSize
  val sexIndustryFirstSize: Int = sexSize * industryFirstSize
  val wifiIndustryFirstSize: Int = wifiSize * industryFirstSize
  val osSlotSize: Int = osSize * slotSize
  val wifiSlotSize: Int = wifiSize * slotSize
  val sexSlotSize: Int = sexSize * slotSize
  val ageSlotSize: Int = ageSize * slotSize
  val provSlotSize: Int = provSize * slotSize

  val metric_fileName = "tracelog/v2/join_data/2016-09-1*/,tracelog/v2/join_data/2016-09-2*/"
  val metric_testFileName = "tracelog/v2/join_data/2016-10-29/"
  val model_feature = "lr2503"

  def getIdx(arr: Array[Int], id: Int): Int = {
    var provIdx = arr.indexOf(id)
    if (provIdx == -1) {
      provIdx = arr.length
    }
    provIdx
  }

  def getIdx(arr: Array[String], id: String): Int = {
    var provIdx = arr.indexOf(id)
    if (provIdx == -1) {
      provIdx = arr.length
    }
    provIdx
  }

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

    val label = r(0).toDouble
    val posDisplay = (parse(r(1)) \ "posDisplay2").extract[Double]
    val posClick = (parse(r(1)) \ "posClick2").extract[Double]
    val userDisplay = (parse(r(1)) \ "userDisplay").extract[Double]
    val userClick = (parse(r(1)) \ "userClick").extract[Double]
    val slot = (parse(r(1)) \ "slotId").extract[String]
    val sex = (parse(r(1)) \ "sex").extract[Int]
    val industryFirstStr = (parse(r(1)) \ "industryFirst").extract[String]
    val os = (parse(r(1)) \ "os").extract[Int]
    val wifiType = (parse(r(1)) \ "wifiType").extract[Int]

    val age = (parse(r(1)) \ "age").extract[Int]
    val provCode = (parse(r(1)) \ "provCode").extract[String]
    val queryTime_hour = (parse(r(1)) \ "queryTime_hour").extract[Int]

    val provIdx = getIdx(provinceArray,provCode)

    val hourIdx = queryTime_hour

    var ageIdx = age
    if(age > 99 || age < 0) {
      ageIdx = 99
    }

    val posDisplayCtrPair = getDisplayCtrIdx(posDisplaySize, posCtrSize, posDisplayUnit, posCtrUnit, posDisplay,posClick)
    val posDisplayPosCtrIdx: Int = calculateCombFeatureWeightIdx(posDisplayCtrPair._1, posDisplayCtrPair._2, posCtrSize)

    val userDisplayCtrPair = getDisplayCtrIdx(userDisplaySize, userCtrSize, userDisplayUnit, userCtrUnit, userDisplay,userClick)
    val userDisplayUserCtrIdx: Int = calculateCombFeatureWeightIdx(userDisplayCtrPair._1, userDisplayCtrPair._2, userCtrSize)

    val sexIdx: Int = getIdx(sexArray,sex)
    val industryFirstIdx: Int = getIdx(industryFirstArray,industryFirstStr)

    val osIdx: Int = getIdx(osArray,os)
    val wifiIdx: Int = getIdx(wifiArray,wifiType)

    val sexIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(sexIdx, industryFirstIdx, industryFirstSize)

    val wifiIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(wifiIdx, industryFirstIdx, industryFirstSize)

    val osIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(osIdx, industryFirstIdx, industryFirstSize)

    val slotIdx: Int = getIdx(slotArray,slot)

    val osSlotIdx: Int = calculateCombFeatureWeightIdx(osIdx, slotIdx, slotSize)

    val wifiSlotIdx: Int = calculateCombFeatureWeightIdx(wifiIdx, slotIdx, slotSize)

    val sexSlotIdx: Int = calculateCombFeatureWeightIdx(sexIdx, slotIdx, slotSize)

    val ageSlotIdx: Int = calculateCombFeatureWeightIdx(ageIdx, slotIdx, slotSize)

    val provSlotIdx: Int = calculateCombFeatureWeightIdx(provIdx, slotIdx, slotSize)

    //    val features = mkFeatureArr(Array(posDisplayPosCtrIdx, userDisplayUserCtrIdx, sexIndustryFirstIdx, slotIdx,
    //      osSlotIdx, wifiSlotIdx, sexSlotIdx, ageSlotIdx, provSlotIdx, wifiIndustryFirstIdx, osIndustryFirstIdx),
    //      Array(posDisplayPosCtrSize, userDisplayUserCtrSize, sexIndustryFirstSize, slotArray.length, osSlotSize,
    //        wifiSlotSize, sexSlotSize, ageSlotSize, provSlotSize, wifiIndustryFirstSize, osIndustryFirstSize))
    //    val featureLen = posDisplayPosCtrSize + userDisplayUserCtrSize + sexIndustryFirstSize + slotArray.length +
    //      osSlotSize + wifiSlotSize + sexSlotSize + ageSlotSize + provSlotSize + wifiIndustryFirstSize + osIndustryFirstSize

    val features = mkFeatureArr(Array(posDisplayPosCtrIdx, userDisplayUserCtrIdx, sexIndustryFirstIdx, slotIdx,
      osSlotIdx, wifiSlotIdx),
      Array(posDisplayPosCtrSize, userDisplayUserCtrSize, sexIndustryFirstSize, slotSize, osSlotSize,
        wifiSlotSize))
    val featureLen = posDisplayPosCtrSize + userDisplayUserCtrSize + sexIndustryFirstSize + slotSize +
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
    val sc = new SparkContext(new SparkConf().setAppName(this.getClass.getName))
    // train

    val rawTrainingData = sc.textFile(metric_fileName).sample(false, 0.5, System.currentTimeMillis().toInt)
    val recordsTrain = rawTrainingData.map(line => line.split("\t"))

    val trainingData = recordsTrain.filter(x => x.length == 2 && x(1).takeRight(1)=="}").map {extractFeature}

    trainingData.cache()
    val metric_trainCount = trainingData.count().toString

    val algorithm = new LogisticRegressionWithLBFGS()
    algorithm.setIntercept(true)

    val model = algorithm.run(trainingData)
    model.clearThreshold()

    val metrics_modelFeatureCnt = model.numFeatures.toString
    val metrics_modelWeights = model.weights.toArray.mkString("\t")
    val metrics_modelIntercept = model.intercept.toString

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
    val recordsTest = rawTestData.map(line => line.split("\t"))
    val testData = recordsTest.filter(x => x.length == 2 && x(1).takeRight(1)=="}").map {extractFeature}

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

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dt = new Date()
    val suffix = dateFormat.format(dt)
    val modelName = suffix + model_feature
    val modelPath = "training_pipeline/model/" + modelName
    model.save(sc, modelPath)
    sc.parallelize(debugBuf,1).saveAsTextFile("training_pipeline/model_metadata/"+modelName)
  }
}
