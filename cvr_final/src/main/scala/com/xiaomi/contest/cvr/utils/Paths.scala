package com.xiaomi.contest.cvr.utils

object Paths {
  // day
  val trainDatasetPath = "/user/h_data_platform/platform/miuiads/contest_dataset_label/data"
  val testDatasetPath = "/user/h_data_platform/platform/miuiads/contest_testset/data"

  val adPath = "/user/h_data_platform/platform/miuiads/contest_dataset_ad/data"
  val userProfilePath = "/user/h_data_platform/platform/miuiads/contest_dataset_user_profile/data"
  val appCategoryPath = "/user/h_data_platform/platform/miuiads/contest_dataset_app_category/data"

  // day
  val appUsagePath = "/user/h_data_platform/platform/miuiads/contest_dataset_app_usage/data"
  val appActionsPath = "/user/h_data_platform/platform/miuiads/contest_dataset_app_actions/data"
  val newsFeedPath = "/user/h_data_platform/platform/miuiads/contest_dataset_newsfeed/data"
  val queryPath = "/user/h_data_platform/platform/miuiads/contest_dataset_query/data"
  val shoppingPath = "/user/h_data_platform/platform/miuiads/contest_dataset_shopping/data"

  val basePath = "/user/h_user_profile/lizhixu/contest/cvr"

  def multipleDaysPath(path: String, end: Int, days: Int): String = {
    0.until(days).map(it => path + "/date=%02d".format(end - it)).reduce(_ + "," + _)
  }

  def dayStr(day: Int): String = {
    "/date=%02d".format(day)
  }
}
