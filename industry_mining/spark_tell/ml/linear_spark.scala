import org.apache.spark.{SparkContext, SparkConf}
val conf = new SparkConf()
val sc = new SparkContext(conf)

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater, LeastSquaresGradient}
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}

val rawTrainingData = sc.textFile("/user/adengine/lai.sijia/posfeature0427.tsv")
val recordsTrain = rawTrainingData.map(line => line.split("\t"))
recordsTrain.first()

val trainingData = recordsTrain.map { r =>
val trimmed = r.map(_.replaceAll(" ",""))
val label = trimmed(0).toDouble
val rawfeatures = trimmed.slice(1,r.size).map(d=> if(d=="") 0.0 else d.toDouble)
val features = Array(rawfeatures(8)*100,rawfeatures(10)*100,rawfeatures(11)*100)
(label, MLUtils.appendBias(Vectors.dense(features)))
}

val numFeatures = trainingData.take(1)(0)._2.size
val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures))

  val numCorrections = 5 //10//5//3
  val convergenceTol = 1e-10 //1e-4
  val maxNumIterations = 20 //20//100
  val regParam = 0.0000001 //0.1//10.0

val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
  trainingData,
  new LeastSquaresGradient(),//LeastSquaresGradient
  new SquaredL2Updater(), //SquaredL2Updater(),SimpleUpdater(),L1Updater()
  numCorrections,
  convergenceTol,
  maxNumIterations,
  regParam,
initialWeightsWithIntercept)

val model = new LinearRegressionModel(
    Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
    weightsWithIntercept(weightsWithIntercept.size - 1))

  println(s">>>> Model intercept: ${model.intercept}, weights: ${model.weights}")

// training auc
val scoreAndLabels = trainingData.map { point =>
  val score = model.predict(Vectors.dense(point._2.toArray.slice(0,weightsWithIntercept.size - 1)))
  (score, point._1)
}

// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

// baseline
val scoreAndLabels4 = trainingData.map { point =>
  val score = point._2(0)
  (score, point._1)
}

val metrics4 = new BinaryClassificationMetrics(scoreAndLabels4)
val auROC4 = metrics4.areaUnderROC()

val testRMetrics4 = new RegressionMetrics(scoreAndLabels4)
val testMSE4 = testRMetrics4.meanSquaredError

// test auc
val rawTestData = sc.textFile("/user/adengine/lai.sijia/posfeature0426.tsv")
val recordsTest = rawTestData.map(line => line.split("\t"))
recordsTest.first()

val testData = recordsTest.map { r =>
val trimmed = r.map(_.replaceAll(" ",""))
val label = trimmed(0).toDouble
val rawfeatures = trimmed.slice(1,r.size).map(d=> if(d=="") 0.0 else d.toDouble)
val features = Array(rawfeatures(8)*100,rawfeatures(10)*100,rawfeatures(11)*100)
(label, MLUtils.appendBias(Vectors.dense(features)))
}

val scoreAndLabels1 = testData.map { point =>
  val score = model.predict(Vectors.dense(point._2.toArray.slice(0,weightsWithIntercept.size - 1)))
  (score, point._1)
}

// Get evaluation metrics.
val metrics1 = new BinaryClassificationMetrics(scoreAndLabels1)
val auROC1 = metrics1.areaUnderROC()
