import org.apache.spark.{SparkContext, SparkConf}
val sc = new SparkContext(new SparkConf())

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}

import org.json4s._
import org.json4s.jackson.JsonMethods._

val sexArray = Array[Integer](1, 2)
val slotArray = Array[String]("2-4", "2-12", "1-3", "1-10", "1-30")
val industryFirstArray = Array[String]("101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117", "118", "119", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129", "999")

val posDisplaySize: Int = 2001
val posCtrSize: Int = 251
val userDisplaySize: Int = 201
val userCtrSize: Int = 501

def calculateCombFeatureWeightIdx(arr1_idx: Int, arr2_idx: Int, arr2_len: Int): Int = {
  if (arr1_idx == -1 || arr2_idx == -1) {
    return -1
  }
  arr1_idx * arr2_len + arr2_idx
}

val rawTrainingData = sc.textFile("tracelog/parse-/2016-09-0[1-9]/,tracelog/parse-/2016-09-1[0-6]/")
val recordsTrain = rawTrainingData.map(line => line.split("\\|"))

// "posDisplay2":690253.0,"posClick2":11901.0, "wifiDisplay":1340139.0,"wifiClick":19190.0,"osDisplay":1340139.0,"osClick":19190.0
// top100 cpc custom
// sketching

val trainingData = recordsTrain.filter(x=>x.length==20 && x(8) == "1").map { r =>
  implicit val formats = DefaultFormats
  var posDisplayIdx: Int = -1
  var posCtrIdx: Int = -1
  var userDisplayIdx: Int = -1
  var userCtrIdx: Int = -1

  val label = r(1).toDouble
  val userDisplay = (parse(r(15)) \ "userDisplay").extract[Double]
  val userClick = (parse(r(15)) \ "userClick").extract[Double]
  val posDisplay = (parse(r(15)) \ "posDisplay").extract[Double]
  val posClick = (parse(r(15)) \ "posClick").extract[Double]

  if (posDisplay <= 0.0) {
    posCtrIdx = posCtrSize - 1
    posDisplayIdx = posDisplaySize - 1
  }
  else {
    posDisplayIdx = (posDisplay / 500.0).toInt
    if (posDisplayIdx >= posDisplaySize - 1) {
      posDisplayIdx = posDisplaySize - 2
    }
    val posCtr: Double = posClick / posDisplay
    posCtrIdx = (posCtr * 10000).toInt
    if (posCtrIdx >= posCtrSize - 1) {
      posCtrIdx = posCtrSize - 2
    }
  }
  val posDisplayPosCtrIdx: Int = calculateCombFeatureWeightIdx(posDisplayIdx, posCtrIdx, posCtrSize)
  val posDisplayPosCtrSize: Int = posDisplaySize * posCtrSize

  if (userDisplay <= 0.0) {
    userCtrIdx = userCtrSize - 1
    userDisplayIdx = userDisplaySize - 1
  }
  else {
    userDisplayIdx = (userDisplay / 50.0).toInt
    if (userDisplayIdx >= userDisplaySize - 1) {
      userDisplayIdx = userDisplaySize - 2
    }
    val userCtr: Double = userClick / userDisplay
    userCtrIdx = (userCtr * 1000).toInt
    if (userCtrIdx >= userCtrSize - 1) {
      userCtrIdx = userCtrSize - 2
    }
  }
  val userDisplayUserCtrIdx: Int = calculateCombFeatureWeightIdx(userDisplayIdx, userCtrIdx, userCtrSize)
  val userDisplayUserCtrSize: Int = userDisplaySize * userCtrSize

  val slot = (parse(r(15)) \ "slotId").extract[String]
  val sex = (parse(r(15)) \ "sex").extract[Int]
  val industryFirstStr = (parse(r(15)) \ "industryFirst").extract[String]

  val sexIdx: Int = sexArray.indexOf(sex)
  val industryFirstIdx: Int = industryFirstArray.indexOf(industryFirstStr)
  val sexIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(sexIdx, industryFirstIdx, industryFirstArray.length)
  val sexIndustryFirstSize: Int = sexArray.length * industryFirstArray.length

  val features = Array(posDisplayPosCtrIdx,userDisplayUserCtrIdx + posDisplayPosCtrSize,sexIndustryFirstIdx + posDisplayPosCtrSize + userDisplayUserCtrSize)
  (slot,LabeledPoint(label,Vectors.sparse(posDisplayPosCtrSize + userDisplayUserCtrSize + sexIndustryFirstSize, features, Array(1,1,1))))
}

trainingData.cache()
trainingData.count()

val algorithm = new LogisticRegressionWithLBFGS()
algorithm.setIntercept(true)
algorithm.optimizer.setUpdater(new L1Updater)
var modelArr = Array[LogisticRegressionModel]()

val model2_4 = algorithm.run(trainingData.filter(x=>x._1=="2-4").map(x=>x._2))
model2_4.clearThreshold()
modelArr = modelArr :+ model2_4
val model2_12 = algorithm.run(trainingData.filter(x=>x._1=="2-12").map(x=>x._2))
model2_12.clearThreshold()
modelArr = modelArr :+ model2_12
val model1_3 = algorithm.run(trainingData.filter(x=>x._1=="1-3").map(x=>x._2))
model1_3.clearThreshold()
modelArr = modelArr :+ model1_3
val model1_10 = algorithm.run(trainingData.filter(x=>x._1=="1-10").map(x=>x._2))
model1_10.clearThreshold()
modelArr = modelArr :+ model1_10
val model1_30 = algorithm.run(trainingData.filter(x=>x._1=="1-30").map(x=>x._2))
model1_30.clearThreshold()
modelArr = modelArr :+ model1_30

// training auc
val predictionsAndLabels = trainingData.map { point =>
  val slot = point._1
  val curModel = modelArr(slotArray.indexOf(slot))
  val prediction = curModel.predict(point._2.features)
  (prediction, point._2.label)
}
val trainauc = new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC
val trainMSE = predictionsAndLabels.map{ case(p, v) => math.pow(v - p, 2)}.mean()
trainingData.unpersist()

val rawTestData = sc.textFile("tracelog/parse-/2016-09-17/")
val recordsTest = rawTestData.map(line => line.split("\\|"))

val testData = recordsTest.filter(x=>x.length==20 && x(8) == "1").map { r =>
  implicit val formats = DefaultFormats
  var posDisplayIdx: Int = -1
  var posCtrIdx: Int = -1
  var userDisplayIdx: Int = -1
  var userCtrIdx: Int = -1

  val label = r(1).toDouble
  val userDisplay = (parse(r(15)) \ "userDisplay").extract[Double]
  val userClick = (parse(r(15)) \ "userClick").extract[Double]
  val posDisplay = (parse(r(15)) \ "posDisplay").extract[Double]
  val posClick = (parse(r(15)) \ "posClick").extract[Double]

  if (posDisplay <= 0.0) {
    posCtrIdx = posCtrSize - 1
    posDisplayIdx = posDisplaySize - 1
  }
  else {
    posDisplayIdx = (posDisplay / 500.0).toInt
    if (posDisplayIdx >= posDisplaySize - 1) {
      posDisplayIdx = posDisplaySize - 2
    }
    val posCtr: Double = posClick / posDisplay
    posCtrIdx = (posCtr * 10000).toInt
    if (posCtrIdx >= posCtrSize - 1) {
      posCtrIdx = posCtrSize - 2
    }
  }
  val posDisplayPosCtrIdx: Int = calculateCombFeatureWeightIdx(posDisplayIdx, posCtrIdx, posCtrSize)
  val posDisplayPosCtrSize: Int = posDisplaySize * posCtrSize

  if (userDisplay <= 0.0) {
    userCtrIdx = userCtrSize - 1
    userDisplayIdx = userDisplaySize - 1
  }
  else {
    userDisplayIdx = (userDisplay / 50.0).toInt
    if (userDisplayIdx >= userDisplaySize - 1) {
      userDisplayIdx = userDisplaySize - 2
    }
    val userCtr: Double = userClick / userDisplay
    userCtrIdx = (userCtr * 1000).toInt
    if (userCtrIdx >= userCtrSize - 1) {
      userCtrIdx = userCtrSize - 2
    }
  }
  val userDisplayUserCtrIdx: Int = calculateCombFeatureWeightIdx(userDisplayIdx, userCtrIdx, userCtrSize)
  val userDisplayUserCtrSize: Int = userDisplaySize * userCtrSize

  val slot = (parse(r(15)) \ "slotId").extract[String]
  val sex = (parse(r(15)) \ "sex").extract[Int]
  val industryFirstStr = (parse(r(15)) \ "industryFirst").extract[String]

  val sexIdx: Int = sexArray.indexOf(sex)
  val industryFirstIdx: Int = industryFirstArray.indexOf(industryFirstStr)
  val sexIndustryFirstIdx: Int = calculateCombFeatureWeightIdx(sexIdx, industryFirstIdx, industryFirstArray.length)
  val sexIndustryFirstSize: Int = sexArray.length * industryFirstArray.length

  val features = Array(posDisplayPosCtrIdx,userDisplayUserCtrIdx + posDisplayPosCtrSize,sexIndustryFirstIdx + posDisplayPosCtrSize + userDisplayUserCtrSize)
  (slot,LabeledPoint(label,Vectors.sparse(posDisplayPosCtrSize + userDisplayUserCtrSize + sexIndustryFirstSize, features, Array(1,1,1))))
}

testData.cache()
testData.count()

// 0 1 2 5 8 11 ud uc ucr pcr hc slot

// Train a GradientBoostedTrees model.
//  The defaultParams for Regression use SquaredError by default.

// test auc
val predictionsAndLabels1 = testData.map { point =>
  val slot = point._1
  val curModel = modelArr(slotArray.indexOf(slot))
  val prediction = curModel.predict(point._2.features)
  (prediction, point._2.label)
}
val testauc = new BinaryClassificationMetrics(predictionsAndLabels1).areaUnderROC

// debug info
val testMSE = predictionsAndLabels1.map{ case(p, v) => math.pow(v - p, 2)}.mean()
println("Test Mean Squared Error = " + testMSE)
testData.unpersist()
// println("Learned regression GBT model:\n" + model.toDebugString)

// model.save(sc, "tempGbdtModel")
// val sameModel = GradientBoostedTreesModel.load(sc, "training_pipeline/gbdt_model/gbdt0811")
/*
val predictionsAndLabels1 = testData.map { point =>
  val prediction = sameModel.predict(point.features)
  (prediction, point.label)
}
val testauc = new BinaryClassificationMetrics(predictionsAndLabels1).areaUnderROC
*/