namespace java com.xiaomi.contest.cvr

struct Label {
  1: optional i32 instance_id;
  2: optional i32 label;
  3: optional string click_time;
  4: optional i32 ad_id;
  5: optional i32 user_id;
  6: optional i32 position_id;
  7: optional string connection_type;
  8: optional i32 miui_version;
  9: optional i32 ip;
  10: optional i32 android_version;
}

struct AdInfo {
  1: optional i32 advertiser_id;
  2: optional i32 campaign_id;
  3: optional i32 ad_id;
  4: optional i32 app_id;
}

struct AppCategory {
  1: optional i32 app_id;
  2: optional list<i32> app_description;
  3: optional i32 app_category1;
  4: optional i32 app_category2;
}

struct UserProfile {
  1: optional i32 user_id;
  2: optional i32 age;
  3: optional i32 gender;
  4: optional string education;
  5: optional i32 province;
  6: optional i32 city;
  7: optional i32 device_info;
  8: optional list<i32> app_installed_list;
}

struct AppUsage {
  1: optional i32 app_id;
  2: optional i32 duration;
  3: optional i32 count;
  4: optional string time;
}

struct UserAppUsage {
  1: optional i32 user_id;
  2: optional list<AppUsage> app_usage_list;
}

struct AppActions {
  1: optional i32 app_id;
  2: optional string action_type;
  3: optional string time;
}

struct UserAppActions {
  1: optional i32 user_id;
  2: optional list<AppActions> app_actions_list;
}

struct Newsfeed {
  1: optional list<i32> tags;
  2: optional string time;
}

struct UserNewsfeed {
  1: optional i32 user_id;
  2: optional list<Newsfeed> news_feed_list;
}

struct Query {
  1: optional list<i32> tags;
  2: optional string time;
}

struct UserQuery {
  1: optional i32 user_id;
  2: optional list<Query> query_list;
}

struct Shopping {
  1: optional list<i32> tags;
  2: optional string time;
}

struct UserShopping {
  1: optional i32 user_id;
  2: optional list<Shopping> shopping_list;
}

struct Counter {
  1: optional i32 clk;
  2: optional i32 label;
}

struct DatasetCounter {
  1: optional map<i32, Counter> advertiser_id;
  2: optional map<i32, Counter> campaign_id;
  3: optional map<i32, Counter> ad_id;
  4: optional map<i32, Counter> app_id;
  5: optional map<i32, Counter> app_category1;
  6: optional map<i32, Counter> app_category2;
  7: optional map<i32, Counter> user_id;
  8: optional map<i32, Counter> ip;
}

struct CurrentCounter {
  1: optional Counter advertiser_id;
  2: optional Counter campaign_id;
  3: optional Counter ad_id;
  4: optional Counter app_id;
  5: optional Counter app_category1;
  6: optional Counter app_category2;
  7: optional Counter user_id;
  8: optional Counter ip;
}

// global stats
struct DatasetHelper {
  1: optional map<i32, AppCategory> appCategory;
}

struct UserHistory {
  1: optional i32 user_id;
  2: optional list<UserAppUsage> app_usage;
  3: optional list<UserAppActions> app_actions;
  4: optional list<UserNewsfeed> news_feed;
  5: optional list<UserQuery> query;
  6: optional list<UserShopping> shopping;
}

// instance_id user stats
struct UserStats {
  1: optional i32 user_id;
  2: optional Counter total;
  3: optional map<i32, Counter> advertiser_id;
  4: optional map<i32, Counter> campaign_id;
  5: optional map<i32, Counter> ad_id;
  6: optional map<i32, Counter> app_id;
  7: optional map<i32, Counter> app_category1;
  8: optional map<i32, Counter> app_category2;
}

// instance_id ip stats
struct IpStats {
  1: optional i32 ip;
  2: optional Counter total;
  3: optional map<i32, Counter> advertiser_id;
  4: optional map<i32, Counter> campaign_id;
  5: optional map<i32, Counter> ad_id;
  6: optional map<i32, Counter> app_id;
  7: optional map<i32, Counter> app_category1;
  8: optional map<i32, Counter> app_category2;
}

// instance_id fea_stats
struct FeaStats {
  1: optional map<string, Counter> stats; // fea group, stats
}

// instance_id ad stats
struct AdStats {
  1: optional i32 ad_id;
  2: optional Counter total;
  3: optional map<i32, Counter> age;
  4: optional map<i32, Counter> gender;
  5: optional map<string, Counter> education;
  6: optional map<i32, Counter> province;
  7: optional map<i32, Counter> city;
  8: optional map<i32, Counter> device_info;
  9: optional map<i32, Counter> app_installed_list;
}

// instance_id app stats
struct AppStats {
  1: optional i32 app_id;
  2: optional Counter total;
  3: optional map<i32, Counter> age;
  4: optional map<i32, Counter> gender;
  5: optional map<string, Counter> education;
  6: optional map<i32, Counter> province;
  7: optional map<i32, Counter> city;
  8: optional map<i32, Counter> device_info;
  9: optional map<i32, Counter> app_installed_list;
}

struct DatasetStats {
  1: optional i32 instance_id;
  2: optional FeaStats fea_stats;
  3: optional UserStats user_stats;
  4: optional IpStats ip_stats;
  5: optional AdStats ad_stats;
  6: optional AppStats app_stats;
}

struct TreeFeatures {
  1: optional i32 instance_id;
  2: optional list<i32> leaf_indices;
}

struct CVRInstance {
  1: optional Label data;
  2: optional AdInfo ad;
  3: optional AppCategory app_category;
  4: optional UserProfile profile;
  5: optional UserHistory history;
  6: optional CurrentCounter counter;
  7: optional DatasetStats data_stats;
  8: optional TreeFeatures tree;
}
