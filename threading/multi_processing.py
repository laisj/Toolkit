import subprocess
import datetime
import multiprocessing
import time


def submit_job(i):
    date = datetime.datetime.fromtimestamp(i).strftime('%Y-%m-%d')
    subStr = "nohup /home/server/bigdata/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --conf spark.cores.max=360 --conf spark.dynamicAllocation.minExecutors=200 spark_gen_trainlog4.py " + date + " " + name
    print subStr
    rep = 0
    while rep < 3:
        process = subprocess.Popen(['timeout', '300', 'bash', '-c', subStr])
        process.wait()
        print date, process.returncode
        if process. returncode == 0:
            print "submited"
            break
        else:
            rep += 1
            suf = "_" * rep
            mv_file_cmd = "/opt/hadoop-2.6.0/bin/hadoop fs -mv training_pipeline/feature_extraction/" + name + "/hash/" + date + " training_pipeline/feature_extraction/" + name + "/hash/" + date + suf
            print mv_file_cmd
            p = subprocess.Popen(mv_file_cmd.split(" "))
            p.wait()
            print "repeat"

start_date_stamp = time.mktime(time.strptime('2018-12-01', "%Y-%m-%d"))
end_date_stamp = time.mktime(time.strptime('2018-12-20', "%Y-%m-%d"))
name = "13_8_7_pool"
name = name.strip()

pool = multiprocessing.Pool(2)

i = start_date_stamp
task_arr = []
while i <= end_date_stamp:
    #msg = "start submit %d" %(i)
    #p = multiprocessing.Process(target=submit_job, args=(i,))
    #p.start()
    #p.join()
    task_arr.append(i)
    i += 86400
print task_arr
pool.map(submit_job, task_arr)

print "all job submited."
pool.close()
pool.join()
print "Sub-process(es) done."