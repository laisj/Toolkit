#encode=utf8
import itertools
import datetime
import time
import operator
import collections

mob_set = set(["134","135","136","137","138","139","147","150","151","152","157","158","159","182","183","184","187","188","130","131","132","145","155","156","185","186","133","153","180","181","189","170","176","177","178"])

keyFunc = lambda f: f[1]
pairs = itertools.groupby((line.split("\t") for line in open(r"d:\sort_june1_user_order.tsv", "r")), keyFunc)

iter = 0
for k,val in pairs:
    if len(k) != 11 or (k[0:3] not in mob_set):
        continue

    v = list(val)
    total_count = len(v)
    order_count = len(set([linearr[13] for linearr in v]))
    self_rate = 1.0 * len(set([linearr[13] for linearr in v if linearr[0] == linearr[26]])) / order_count
#    print total_count, order_count, self_rate, v[1][20], v[1][28], time.mktime(datetime.datetime.strptime(v[1][28].strip(), "%Y%m%d").timetuple())
    xcd_rate = 1.0 *  len(set([linearr[13] for linearr in v if linearr[9] == '1'])) / order_count
    day_0_rate = 1.0 *  len(set([linearr[13] for linearr in v if time.mktime(datetime.datetime.strptime(linearr[20], "%Y-%m-%d").timetuple()) - time.mktime(datetime.datetime.strptime(linearr[28].strip(), "%Y%m%d").timetuple()) == 0])) / order_count
    day_1_rate = 1.0 *  len(set([linearr[13] for linearr in v if time.mktime(datetime.datetime.strptime(linearr[20], "%Y-%m-%d").timetuple()) - time.mktime(datetime.datetime.strptime(linearr[28].strip(), "%Y%m%d").timetuple()) == 86400])) / order_count
    day_2_rate = 1.0 *  len(set([linearr[13] for linearr in v if time.mktime(datetime.datetime.strptime(linearr[20], "%Y-%m-%d").timetuple()) - time.mktime(datetime.datetime.strptime(linearr[28].strip(), "%Y%m%d").timetuple()) == 2*86400])) / order_count
    day_gt2_rate = 1.0 *  len(set([linearr[13] for linearr in v if time.mktime(datetime.datetime.strptime(linearr[20], "%Y-%m-%d").timetuple()) - time.mktime(datetime.datetime.strptime(linearr[28].strip(), "%Y%m%d").timetuple()) > 2*86400])) / order_count
    average_person_per_order = 1.0 * len(set([linearr[26] for linearr in v])) / order_count
    dep_city_count = len(set([linearr[18] for linearr in v]))
    arr_city_count = len(set([linearr[15] for linearr in v]))
    line_count = len(set([linearr[15] + "_" + linearr[18] for linearr in v]))
    airlines = collections.defaultdict(int)
    xcd_adds = collections.defaultdict(int)
    dep_times = collections.defaultdict(int)
    airno = collections.defaultdict(int)
    aircoms = collections.defaultdict(int)
    for linearr in v:
        airlines[linearr[15] + "_" + linearr[18]] += 1
        xcd_adds[linearr[-2]] += 1
        dep_times[linearr[-8]] += 1
        airno[linearr[-7]] += 1
        aircoms[linearr[-7][0:2]] += 1

    top_10_lines =reduce(lambda x,y: x+ "<>" +y,[x[0]+"|"+str(x[1]) for x in list(sorted(airlines.items(), key=operator.itemgetter(1), reverse=True))[0:10]])
    top_10_xcd_address = reduce(lambda x,y: x+ "<>" +y,[x[0]+"|"+str(x[1]) for x in list(sorted(xcd_adds.items(), key=operator.itemgetter(1), reverse=True))[0:10]])
    top_10_time_bucket_array = reduce(lambda x,y: x+ "<>" +y,[x[0]+"|"+str(x[1]) for x in list(sorted(dep_times.items(), key=operator.itemgetter(1), reverse=True))[0:10]])
    top_10_airno = reduce(lambda x,y: x+ "<>" +y,[x[0]+"|"+str(x[1]) for x in list(sorted(airno.items(), key=operator.itemgetter(1), reverse=True))[0:10]])
    top_10_aircom = reduce(lambda x,y: x+ "<>" +y,[x[0]+"|"+str(x[1]) for x in list(sorted(aircoms.items(), key=operator.itemgetter(1), reverse=True))[0:10]])

    print "\t".join(map(lambda x: str(x),(total_count, order_count, self_rate, xcd_rate,day_0_rate, day_1_rate, day_2_rate, day_gt2_rate, average_person_per_order, dep_city_count, arr_city_count, line_count, top_10_lines, top_10_xcd_address, top_10_time_bucket_array, top_10_airno, top_10_aircom)))
    if iter > 300:
        break
    iter += 1
