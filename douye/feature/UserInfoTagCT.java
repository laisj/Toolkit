package com.xiaomi.miui.ad.predict.feature;

import com.xiaomi.miui.ad.predict.data.HistoricalDataCorpus;
import com.xiaomi.miui.ad.predict.thrift.model.*;
import com.xiaomi.miui.ad.predict.util.FeatureCollector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created: wwxu(xuwenwen@xiaomi.com)
 * Date: 2017-09-13
 */
public class UserInfoTagCT extends BaseFeature {
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureManager.class);

    private static String discretizeVal(String feaName, Long feaVal) {
        String value = String.valueOf(feaName) + "_";
        if (feaVal == 0) {
            value += "0";
        } else {
            Double uTime = Math.log(feaVal);
            value += String.valueOf(uTime.intValue() + 1);
        }
        return value;
    }

    private static String discretizeVal(int feaVal) {
        String value = "0";
        if (feaVal > 0) {
            Double uTime = Math.log(feaVal);
            value = String.valueOf(uTime.intValue() + 1);
        }
        return value;
    }

    private static void put2Map(String val, String key, Map<String, List<String>> map) {
        if (StringUtils.isBlank(val)) {
            return;
        }
        List<String> list = new ArrayList<>();
        Set<String> set = new HashSet<>();
        String[] segs = val.split(",");
        for (int i = 0 ; i < segs.length; i++) {
            if (!set.contains(segs[i])) {
                list.add(segs[i]);
                set.add(segs[i]);
            }
        }
        map.put(key, list);
    }

    public static Map<String, List<String>> genUserMap(UserData userData)  {
        Map<String, List<String>> userMap = new HashMap<String, List<String>>();
        put2Map(userData.getAge().toString(), "usag", userMap);
        put2Map(userData.getGender().toString(), "usgd", userMap);
        put2Map(String.valueOf(userData.getProvinceCode()), "uspv", userMap);
        put2Map(userData.getCity(), "usct", userMap);

        String degreeStr = "UNKNOWN";
        if (!userData.getDegreeStr().equals("\\N")) {
            degreeStr = userData.getDegreeStr();
        }
        put2Map(degreeStr, "usdg", userMap);
        put2Map(String.valueOf(userData.getDeviceInfo()), "usdi", userMap);

        // app install
        if(userData.isSetIApps() && userData.getIApps().isSetInstalledApps()
                && userData.getIApps().getInstalledApps().size() > 0) {
            List<String> list = new ArrayList<>();
            List<Long> installed = userData.getIApps().getInstalledApps();
            for (Long appId : installed) {
                list.add(String.valueOf(appId));
            }
            userMap.put("usia", list);
            put2Map(discretizeVal(installed.size()), "usin", userMap);

        }

        // app usage
        if (userData.isSetAppUsageData() && userData.getAppUsageData().isSetAppUsageMap()
                && userData.getAppUsageData().getAppUsageMapSize()> 0) {
            Map<Integer, List<AppUsageInfo>> map = userData.getAppUsageData().getAppUsageMap();
            for (Map.Entry<Integer, List<AppUsageInfo>> entry : map.entrySet()) {
                String daySpan = String.valueOf(entry.getKey());
                List<AppUsageInfo> list = entry.getValue();
                List<String> usua = new ArrayList<String>();
                List<String> usut = new ArrayList<String>();
                List<String> usuo = new ArrayList<String>();
                for (AppUsageInfo appUsageInfo : list) {
                    if (appUsageInfo.getUtime() < 30 || usua.size() >= 80) {
                        continue;
                    }
                    usua.add(String.valueOf(appUsageInfo.getAppId()));
                    String value = discretizeVal(String.valueOf(appUsageInfo.getAppId()), appUsageInfo.getUtime());
                    usut.add(value);
                    value = discretizeVal(String.valueOf(appUsageInfo.getAppId()), appUsageInfo.getOtime());
                    usuo.add(value);
                }
                userMap.put("usua_" + daySpan, usua);
//                userMap.put("usut_" + daySpan, usut);
//                userMap.put("usuo_" + daySpan, usuo);
                put2Map(String.valueOf(usua.size()), "usun_" + daySpan, userMap);
            }
        }

        // app action
        if (userData.isSetAppActionInfoData() && userData.getAppActionInfoData().isSetAppActionMap()
                && userData.getAppActionInfoData().getAppActionMapSize() > 0) {
            Map<Integer, List<AppActionInfo>> map = userData.getAppActionInfoData().getAppActionMap();
            for (Map.Entry<Integer, List<AppActionInfo>> entry : map.entrySet()) {
                String daySpan = String.valueOf(entry.getKey());
                List<AppActionInfo> list = entry.getValue();
                List<String> usai = new ArrayList<String>();
                List<String> usau = new ArrayList<String>();
                List<String> usad = new ArrayList<String>();
                for (AppActionInfo appActionInfo : list) {
                    String actionType = appActionInfo.getActionType();
                    String value = appActionInfo.getAppId() + "_" + String.valueOf(appActionInfo.getCounts());
                    if ("INSTALLED".equals(actionType)) {
                        usai.add(value);
                    } else if ("UNINSTALLED".equals(actionType)) {
                        usau.add(value);
                    } else {
                        usad.add(value);
                    }
                }
                userMap.put("usai_" + daySpan, usai);
                put2Map(discretizeVal(usai.size()), "usa1_" + daySpan, userMap);
                userMap.put("usau_" + daySpan, usau);
                put2Map(discretizeVal(usau.size()), "usa2_" + daySpan, userMap);
                userMap.put("usad_" + daySpan, usad);
                put2Map(discretizeVal(usad.size()), "usa3_" + daySpan, userMap);
            }
        }

        // activate behavior
        if (userData.isSetActivateBehavior() && userData.getActivateBehavior().isSetActivateMap()
                && !userData.getActivateBehavior().getActivateMap().isEmpty()) {
            for (Map.Entry<Integer, ActivateStat> entry : userData.getActivateBehavior().getActivateMap().entrySet()) {
                String daySpan = String.valueOf(entry.getKey());
                List<String> usrt = new ArrayList<String>();
                List<String> usrs = new ArrayList<String>();
                List<String> usru = new ArrayList<String>();
                if (entry.getValue() != null) {
                    ActivateStat activateStat = entry.getValue();
                    Long sd = activateStat.getStartDownloadCount();
                    Long au = activateStat.getAppUsageCount();
                    if (sd+au == 0) {
                        continue;
                    }
                    //int rate = (int)((au*1.0/sd) * 100);
                    String value = String.valueOf(activateStat.getStartDownloadCount()) + "_" + String.valueOf(activateStat.getAppUsageCount());
                    //String value = String.valueOf(activateStat.getStartDownloadCount()) + "_" + String.valueOf(rate);
                    usrt.add(value);
                    usrs.add(String.valueOf(sd));
                    usru.add(String.valueOf(au));
                }
                userMap.put("usrt_" + daySpan, usrt);
                userMap.put("usrs_" + daySpan, usrs);
                userMap.put("usru_" + daySpan, usru);
            }
        }

        // query behavior
//        if (userData.isSetAppQueryData()) {
//            AppQueryData appQueryData = userData.getAppQueryData();
//            if (appQueryData.isSetAppQueryMap() && appQueryData.getAppQueryMapSize() > 0) {
//                for (Map.Entry<Integer,List<String>> entry : appQueryData.getAppQueryMap().entrySet()) {
//                    String daySpan = String.valueOf(entry.getKey());
//                    List<String> list = new ArrayList<>();
//                    for (int i = 0; i < Math.min(80, entry.getValue().size()); i++) {
//                        list.add(entry.getValue().get(i));
//                    }
//                    userMap.put("usqa_" + daySpan, list);
//                }
//            }
//            if (appQueryData.isSetC1QueryMap() && appQueryData.getC1QueryMapSize() > 0) {
//                for (Map.Entry<Integer,List<String>> entry : appQueryData.getC1QueryMap().entrySet()) {
//                    String daySpan = String.valueOf(entry.getKey());
//                    userMap.put("usq1_" + daySpan, entry.getValue());
//                }
//            }
//            if (appQueryData.isSetC2QueryMap() && appQueryData.getC2QueryMapSize() > 0) {
//                for (Map.Entry<Integer,List<String>> entry : appQueryData.getC2QueryMap().entrySet()) {
//                    String daySpan = String.valueOf(entry.getKey());
//                    userMap.put("usq2_" + daySpan, entry.getValue());
//                }
//            }
//         }

        return userMap;
    }


    public FeatureCollector collect(UserData userData, AdData adData, ContextData contextData, HistoricalDataCorpus histData, FeatureCollector collector) {
        if (userData == null) {
            LOGGER.warn("user data null");
            return collector;
        }

        String tagId = contextData.getTagId();
        LOGGER.debug("UserInfo feature extraction start");
        StringBuilder feature = new StringBuilder(64);
        Map<String, List<String>> userMap = genUserMap(userData);

        for (Map.Entry<String, List<String>> user : userMap.entrySet()) {
            for (String userVal : user.getValue()) {
                feature.append(user.getKey()).append(".").append(userVal);
         //              .append(".").append(tagId);
                collector.add(feature.toString());
                feature.setLength(0);
            }
        }


        LOGGER.debug("UserInfo feature extraction end");
        return collector;
    }
}
