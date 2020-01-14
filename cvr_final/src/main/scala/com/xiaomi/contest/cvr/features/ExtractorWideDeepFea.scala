package com.xiaomi.contest.cvr.features

import com.xiaomi.contest.cvr.group.FeaGroup
import com.xiaomi.contest.cvr.samples.Sample
import com.xiaomi.contest.cvr.{CVRInstance, DatasetHelper}

import scala.collection.JavaConversions._

/**
  * Created by Axiom on 16/5/9.
  */
object ExtractorWideDeepFea {
  def single(instance: CVRInstance, hash: DatasetHelper, idx: Int, sameCount: Int, dupAd: Boolean, dupUser: Boolean): Sample = {
    val sameSampleFeatures = ExtractorDnnFea.extractSameSampleFeatures(instance, idx, sameCount, dupAd, dupUser)
    val counterFeatures = ExtractorDnnFea.extractCounterFeatures(instance)
    val features = ExtractorDnnFea.extractFeatures(instance, hash)
    val treeFeatures = ExtractorDnnFea.extractTreeFeatures(instance)
    val statsFeatures = ExtractorStatsFea.extractStatsFea(instance)

    val ans = new Sample()
    ans.setLabel(instance.getData.getLabel)
    ans.setInstance_id(instance.getData.getInstance_id)
    ans.setGroup_number(FeaGroup.values().length)
    ans.setFeatures(sameSampleFeatures ++ counterFeatures ++ features ++ statsFeatures)
    ans.setTree_features(treeFeatures)
    ans.setTime(instance.getData.getClick_time)
    ans
  }

  def singleNoStats(instance: CVRInstance, hash: DatasetHelper, idx: Int, sameCount: Int, dupAd: Boolean, dupUser: Boolean): Sample = {
    val sameSampleFeatures = ExtractorDnnFea.extractSameSampleFeatures(instance, idx, sameCount, dupAd, dupUser)
    val counterFeatures = ExtractorDnnFea.extractCounterFeatures(instance)
    val features = ExtractorDnnFea.extractFeatures(instance, hash)
    val treeFeatures = ExtractorDnnFea.extractTreeFeatures(instance)

    val ans = new Sample()
    ans.setLabel(instance.getData.getLabel)
    ans.setInstance_id(instance.getData.getInstance_id)
    ans.setGroup_number(FeaGroup.values().length)
    ans.setFeatures(sameSampleFeatures ++ counterFeatures ++ features)
    ans.setTree_features(treeFeatures)
    ans.setTime(instance.getData.getClick_time)
    ans
  }
}
