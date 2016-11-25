import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater, LeastSquaresGradient}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import org.json4s._
import org.json4s.jackson.JsonMethods._

val rawTrainingData = sc.textFile("tracelog/parse-/2016-08-0[7-9]/,tracelog/parse-/2016-08-1[0-5]/").repartition(1000).cache()
val recordsTrain = rawTrainingData.map(line => line.split("\\|"))

val trainingData = recordsTrain.filter(x=>x.length==20 && x(8) == "1").map { r =>
  implicit val formats = DefaultFormats
  val label = r(1).toDouble
  val userDisplay = (parse(r(15)) \ "userDisplay").extract[Double]
  val userClick = (parse(r(15)) \ "userClick").extract[Double]
  val posDisplay = (parse(r(15)) \ "posDisplay").extract[Double]
  val posClick = (parse(r(15)) \ "posClick").extract[Double]
  val display_0hour = (parse(r(15)) \ "display_0hour").extract[Double]
  val click_0hour = (parse(r(15)) \ "click_0hour").extract[Double]
  val queryTime_hour = (parse(r(15)) \ "queryTime_hour").extract[Double]
  val os = (parse(r(15)) \ "os").extract[Double]
  val slot = (parse(r(15)) \ "slotId").extract[String]
  var slotId = -1
  if (Array("2-4","2-12","1-3","1-10","1-30").contains(slot)) {
    slotId = Array("2-4","2-12","1-3","1-10","1-30").indexOf(slot)
  }
  val s24 = if (slot == "2-4") 1.0 else 0.0
  val s212 = if (slot == "2-12") 1.0 else 0.0
  val s13 = if (slot == "1-3") 1.0 else 0.0
  val s110 = if (slot == "1-10") 1.0 else 0.0
  val s130 = if (slot == "1-30") 1.0 else 0.0
  val userCtr = if (userDisplay == 0.0) -1 else userClick/userDisplay
  val posCtr = if (posDisplay == 0.0) -1 else posClick/posDisplay
  val hourCtr = if (display_0hour == 0.0) -1 else click_0hour/display_0hour
  val wifiType = (parse(r(15)) \ "wifiType").extract[Double]
  val sex = (parse(r(15)) \ "sex").extract[Double]
  val age = (parse(r(15)) \ "age").extract[Double]
  val industryFirstStr = (parse(r(15)) \ "industryFirst").extract[String]
  val industryFirst = try { industryFirstStr.toDouble } catch {case _ => -1.0 }
  val features = Array(userDisplay, userClick, userCtr, posDisplay, posClick, posCtr, display_0hour, click_0hour, hourCtr, queryTime_hour, os, slotId, wifiType, sex, age, industryFirst,s24,s212,s13,s110,s130)
  LabeledPoint(label, Vectors.dense(features))
}

val rawTestData = sc.textFile("tracelog/parse-/2016-08-17/").repartition(1000).cache()
val recordsTest = rawTestData.map(line => line.split("\\|"))

val testData = recordsTest.filter(x=>x.length==20 && x(8) == "1").map { r =>
  implicit val formats = DefaultFormats
  val label = r(1).toDouble
  val userDisplay = (parse(r(15)) \ "userDisplay").extract[Double]
  val userClick = (parse(r(15)) \ "userClick").extract[Double]
  val posDisplay = (parse(r(15)) \ "posDisplay").extract[Double]
  val posClick = (parse(r(15)) \ "posClick").extract[Double]
  val display_0hour = (parse(r(15)) \ "display_0hour").extract[Double]
  val click_0hour = (parse(r(15)) \ "click_0hour").extract[Double]
  val queryTime_hour = (parse(r(15)) \ "queryTime_hour").extract[Double]
  val os = (parse(r(15)) \ "os").extract[Double]
  val slot = (parse(r(15)) \ "slotId").extract[String]
  var slotId = -1
  if (Array("2-4","2-12","1-3","1-10","1-30").contains(slot)) {
    slotId = Array("2-4","2-12","1-3","1-10","1-30").indexOf(slot)
  }
  val s24 = if (slot == "2-4") 1.0 else 0.0
  val s212 = if (slot == "2-12") 1.0 else 0.0
  val s13 = if (slot == "1-3") 1.0 else 0.0
  val s110 = if (slot == "1-10") 1.0 else 0.0
  val s130 = if (slot == "1-30") 1.0 else 0.0
  val userCtr = if (userDisplay == 0.0) -1 else userClick/userDisplay
  val posCtr = if (posDisplay == 0.0) -1 else posClick/posDisplay
  val hourCtr = if (display_0hour == 0.0) -1 else click_0hour/display_0hour
  val wifiType = (parse(r(15)) \ "wifiType").extract[Double]
  val sex = (parse(r(15)) \ "sex").extract[Double]
  val age = (parse(r(15)) \ "age").extract[Double]
  val industryFirstStr = (parse(r(15)) \ "industryFirst").extract[String]
  val industryFirst = try { industryFirstStr.toDouble } catch {case _ => -1.0 }
  val features = Array(userDisplay, userClick, userCtr, posDisplay, posClick, posCtr, display_0hour, click_0hour, hourCtr, queryTime_hour, os, slotId, wifiType, sex, age, industryFirst,s24,s212,s13,s110,s130)
  LabeledPoint(label, Vectors.dense(features))
}

// 0 1 2 5 8 11 ud uc ucr pcr hc slot

// Train a GradientBoostedTrees model.
//  The defaultParams for Regression use SquaredError by default.

//boostingStrategy.setTreeStrategy(ts)
//boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
//boostingStrategy.treeStrategy.maxDepth = 5
//Empty categoricalFeaturesInfo indicates all features are continuous.
//boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

var boostingStrategy = BoostingStrategy.defaultParams("Regression")
//boostingStrategy.setNumIterations(50)
boostingStrategy.setValidationTol(0.00001)
//boostingStrategy.getTreeStrategy.setMaxDepth(10)
//boostingStrategy.getTreeStrategy.setMinInstancesPerNode(10)
val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

// training auc
val predictionsAndLabels = trainingData.map { point =>
val prediction = model.predict(point.features)
    (prediction, point.label)
}
val trainauc = new BinaryClassificationMetrics(predictionsAndLabels).areaUnderROC

// test auc
val predictionsAndLabels1 = testData.map { point =>
val prediction = model.predict(point.features)
    (prediction, point.label)
}
val testauc = new BinaryClassificationMetrics(predictionsAndLabels1).areaUnderROC

// baseline auc
val predictionsAndLabels4 = trainingData.map { point =>
  val prediction = point.features(3)
  (prediction, point.label)
}
val baseauc = new BinaryClassificationMetrics(predictionsAndLabels4).areaUnderROC


// debug info
val testMSE = predictionsAndLabels.map{ case(p, v) => math.pow(v - p, 2)}.mean()
println("Test Mean Squared Error = " + testMSE)
println("Learned regression GBT model:\n" + model.toDebugString)

model.save(sc, "tempGbdtModel")
val sameModel = GradientBoostedTreesModel.load(sc, "training_pipeline/gbdt_model/gbdt0811")
/*
val predictionsAndLabels1 = testData.map { point =>
  val prediction = sameModel.predict(point.features)
  (prediction, point.label)
}
val testauc = new BinaryClassificationMetrics(predictionsAndLabels1).areaUnderROC
*/