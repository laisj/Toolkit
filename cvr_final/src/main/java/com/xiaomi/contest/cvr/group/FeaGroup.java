package com.xiaomi.contest.cvr.group;

/**
 * Created by Axiom on 16/5/9.
 */
public enum FeaGroup {
  pos,
  connection_type, miui_version, ip, android_version,
  advertiser_id, campaign_id, ad_id, app_id,
  app_description,
  app_category1,
  app_category2,

  clk_day, clk_hour,

  clk_day_pos, clk_day_connection_type,
  clk_day_advertiser_id, clk_day_campaign_id, clk_day_ad_id,
  clk_day_app_id, clk_day_app_description, clk_day_app_category1, clk_day_app_category2,

  clk_hour_pos, clk_hour_connection_type,
  clk_hour_advertiser_id, clk_hour_campaign_id, clk_hour_ad_id,
  clk_hour_app_id, clk_hour_app_description, clk_hour_app_category1, clk_hour_app_category2,

  pos_connection_type, pos_miui_version, pos_ip, pos_android_version,
  pos_advertiser_id, pos_campaign_id, pos_ad_id,
  pos_app_id, pos_app_description, pos_app_category1, pos_app_category2,
  pos_user_id, pos_age, pos_gender, pos_education, pos_province, pos_city, pos_device_info,
  pos_age_gender,

  connection_type_miui_version, connection_type_ip, connection_type_android_version,
  connection_type_advertiser_id, connection_type_campaign_id, connection_type_ad_id,
  connection_type_app_id, connection_type_app_description, connection_type_app_category1, connection_type_app_category2,
  connection_type_user_id, connection_type_age, connection_type_gender, connection_type_education, connection_type_province, connection_type_city, connection_type_device_info,
  connection_type_age_gender,

  stats_fea_cvr,
  stats_fea_cvr_count,

  stats_user_total_cvr, stats_user_total_cvr_count,
  stats_user_advertiser_cvr, stats_user_campaign_cvr, stats_user_ad_cvr, stats_user_app_cvr,
  stats_user_app_category1_cvr, stats_user_app_category2_cvr,
  stats_user_advertiser_cvr_count, stats_user_campaign_cvr_count, stats_user_ad_cvr_count, stats_user_app_cvr_count,
  stats_user_app_category1_cvr_count, stats_user_app_category2_cvr_count,

  stats_ip_total_cvr, stats_ip_total_cvr_count,
  stats_ip_advertiser_cvr, stats_ip_campaign_cvr, stats_ip_ad_cvr, stats_ip_app_cvr,
  stats_ip_app_category1_cvr, stats_ip_app_category2_cvr,
  stats_ip_advertiser_cvr_count, stats_ip_campaign_cvr_count, stats_ip_ad_cvr_count, stats_ip_app_cvr_count,
  stats_ip_app_category1_cvr_count, stats_ip_app_category2_cvr_count,

  stats_ad_total_cvr, stats_ad_total_cvr_count,
  stats_ad_age_cvr, stats_ad_age_cvr_count,
  stats_ad_gender_cvr, stats_ad_gender_cvr_count,
  stats_ad_education_cvr, stats_ad_education_cvr_count,
  stats_ad_province_cvr, stats_ad_province_cvr_count,
  stats_ad_city_cvr, stats_ad_city_cvr_count,
  stats_ad_device_info_cvr, stats_ad_device_info_cvr_count,

  stats_app_total_cvr, stats_app_total_cvr_count,
  stats_app_age_cvr, stats_app_age_cvr_count,
  stats_app_gender_cvr, stats_app_gender_cvr_count,
  stats_app_education_cvr, stats_app_education_cvr_count,
  stats_app_province_cvr, stats_app_province_cvr_count,
  stats_app_city_cvr, stats_app_city_cvr_count,
  stats_app_device_info_cvr, stats_app_device_info_cvr_count,

  advertiser_id_cvr, campaign_id_cvr, ad_id_cvr, app_id_cvr,
  app_category1_cvr, app_category2_cvr,
  user_id_cvr, ip_cvr,

  advertiser_id_cvr_count, campaign_id_cvr_count, ad_id_cvr_count, app_id_cvr_count,
  app_category1_cvr_count, app_category2_cvr_count,
  user_id_cvr_count, ip_cvr_count,

  advertiser_id_first, campaign_id_first, ad_id_first, app_id_first,
  app_category1_first, app_category2_first,
  user_id_first, ip_first,

  user_id, age, gender, education, province, city, device_info,

  app_installed,
  app_installed_count,

  app_installed_match,
  app_installed_match_count,

  app_installed_category1,
  app_installed_category1_ratio,
  app_installed_category1_match,
  app_installed_category1_match_ratio,

  app_installed_category2,
  app_installed_category2_ratio,
  app_installed_category2_match,
  app_installed_category2_match_ratio,

  miui_version_advertiser_id, miui_version_campaign_id, miui_version_ad_id,
  miui_version_app_id, miui_version_app_description, miui_version_app_category1, miui_version_app_category2,

  android_version_advertiser_id, android_version_campaign_id, android_version_ad_id,
  android_version_app_id, android_version_app_description, android_version_app_category1, android_version_app_category2,

  age_gender,
  age_gender_advertiser_id, age_gender_campaign_id, age_gender_ad_id,
  age_gender_app_id, age_gender_app_description, age_gender_app_category1, age_gender_app_category2,

  age_advertiser_id, age_campaign_id, age_ad_id,
  age_app_id, age_app_description, age_app_category1, age_app_category2,

  gender_advertiser_id, gender_campaign_id, gender_ad_id,
  gender_app_id, gender_app_description, gender_app_category1, gender_app_category2,

  education_advertiser_id, education_campaign_id, education_ad_id,
  education_app_id, education_app_description, education_app_category1, education_app_category2,

  province_advertiser_id, province_campaign_id, province_ad_id,
  province_app_id, province_app_description, province_app_category1, province_app_category2,

  city_advertiser_id, city_campaign_id, city_ad_id,
  city_app_id, city_app_description, city_app_category1, city_app_category2,

  device_info_advertiser_id, device_info_campaign_id, device_info_ad_id,
  device_info_app_id, device_info_app_description, device_info_app_category1, device_info_app_category2,

  app_usage,
  app_usage_count,
  app_usage_duration,
  app_usage_avg_duration,

  app_usage_category1,
  app_usage_category1_count,
  app_usage_category1_duration,
  app_usage_category1_avg_duration,

  app_usage_category2,
  app_usage_category2_count,
  app_usage_category2_duration,
  app_usage_category2_avg_duration,

  app_usage_match,
  app_usage_match_count,
  app_usage_match_duration,
  app_usage_match_avg_duration,

  app_usage_category1_match,
  app_usage_category1_match_count,
  app_usage_category1_match_duration,
  app_usage_category1_match_avg_duration,

  app_usage_category2_match,
  app_usage_category2_match_count,
  app_usage_category2_match_duration,
  app_usage_category2_match_avg_duration,

  app_actions,
  app_actions_category1,
  app_actions_category2,
  app_actions_match,
  app_actions_category1_match,
  app_actions_category2_match,

  news_tags, news_tags_count,

  query_tags, query_tags_count,

  shopping_tags, shopping_tags_count,

  user_ad_cnt,
  same_with_last,
  last_click_time_diff,

  cvr_cnt,
  same,
  same_cnt,
  same_before,
  same_first, same_mid, same_last,
  leaf_indices,

  test_dup_ad,
  test_dup_user,
}
