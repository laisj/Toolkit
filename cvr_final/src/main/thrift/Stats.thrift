namespace java com.xiaomi.contest.cvr.stats

struct FeatureStats {
  1: optional i32 cnt;
  2: optional list<double> value;
  3: optional double min;
  4: optional double max;
  5: optional double mean;
  6: optional double stddev;
}

struct SampleStats {
  1: optional map<string, i32> featureId;
  2: optional map<string, FeatureStats> featureStats;
}
