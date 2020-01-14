package com.xiaomi.contest.cvr.features

import com.xiaomi.contest.cvr.samples.Sample
import com.xiaomi.contest.cvr.utils.FeaUtils._
import com.xiaomi.contest.cvr.{CVRInstance, DatasetHelper}

import scala.collection.JavaConversions._

/**
  * Created by Axiom on 16/5/9.
  */
object ExtractorDnnStatsFea {
  def single(instance: CVRInstance, hash: DatasetHelper, idx: Int, sameCount: Int, dupAd: Boolean, dupUser: Boolean): Sample = {
    val sameSampleFeatures = ExtractorDnnFea.extractSameSampleFeatures(instance, idx, sameCount, dupAd, dupUser)
    val treeFeatures = ExtractorDnnFea.extractTreeFeatures(instance)
    val counterFeatures = ExtractorDnnFea.extractCounterFeatures(instance)
    val features = ExtractorDnnFea.extractFeatures(instance, hash)
    val statsFeatures = ExtractorStatsFea.extractStatsFea(instance)

    val ans = sameSampleFeatures ++ treeFeatures ++ counterFeatures ++ features ++ statsFeatures
    makeSample(ans, instance)
  }

  def singleNoTree(instance: CVRInstance, hash: DatasetHelper, idx: Int, sameCount: Int, dupAd: Boolean, dupUser: Boolean): Sample = {
    val sameSampleFeatures = ExtractorDnnFea.extractSameSampleFeatures(instance, idx, sameCount, dupAd, dupUser)
    val counterFeatures = ExtractorDnnFea.extractCounterFeatures(instance)
    val features = ExtractorDnnFea.extractFeatures(instance, hash)
    val statsFeatures = ExtractorStatsFea.extractStatsFea(instance)

    val ans = sameSampleFeatures ++ counterFeatures ++ features ++ statsFeatures
    makeSample(ans, instance)
  }
}
