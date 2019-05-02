#!/usr/bin/python

import os
import io
import glob
import requests
import sys
import time
import zipfile
import gzip
from subprocess import call
import multiprocessing as mp


interpreter = sys.executable

dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

asterixdb = os.path.join(dir_path, "asterix-server", "opt", "local")
ycsb = os.path.join(dir_path, "ycsb-asterixdb-binding-0.16.0-SNAPSHOT", "bin", "ycsb")
logs_dir = os.path.join(dir_path, "logs")
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)


def ycsb_job(is_load, workload, num_threads, ops):
    if is_load:
        load_str = "load"
    else:
        load_str = "run"
    if num_threads == 1:
        thread_str = ""
    else:
        thread_str = " -threads " + str(num_threads)
    if ops < 1:
        ops_str = ""
    else:
        ops_str = " -target " + str(ops)
    basename = workload + "_slow_"
    cmd = interpreter + " \"" + ycsb + "\" " + load_str + " asterixdb -P \"" + \
          os.path.join(dir_path, "workloads", workload + ".properties") + "\"" + \
          " -p exportfile=\"" + os.path.join(logs_dir, basename + "final.log") + \
          "\" -s" + thread_str + ops_str + " &> \"" + \
          os.path.join(logs_dir, basename + "realtime.log") + "\""
    call(cmd, shell=True)


def list_files(delay):
    def print_size(s):
        if s < 1024:
            return  "{}b".format(s)
        elif s < 1024 ** 2:
            return "{:.1f}k".format(round(s / 1024))
        elif s < 1024 ** 3:
            return "{:.1f}m".format(round(s / (1024 ** 2)))
        else:
            return "{:.1f}g".format(round(s / (1024 ** 3)))

    data_dir = os.path.join(asterixdb, "data", "red", "storage", "partition_0", "ycsb", "usertable",
                            "0", "usertable")

    while True:
        if os.path.isdir(data_dir):
            files = []
            num_files = 0
            total_size = 0
            for f in sorted(glob.glob(os.path.join(data_dir, "*_b"))):
                try:
                    size = int(os.path.getsize(f))
                    files.append("{}: {}".format(os.path.basename(f), print_size(size)))
                    num_files += 1
                    total_size += size
                except:
                    pass
            print("Total {}: {}".format(num_files, print_size(total_size)))
            if num_files > 0:
                print(", ".join(files))
        time.sleep(delay)


def start_server():
    call("\"" + asterixdb + "/bin/start-sample-cluster.sh\"", shell=True)


def stop_server():
    call("\"" + asterixdb + "/bin/stop-sample-cluster.sh\" -f", shell=True)


query_url = ""
feed_port = 0

with io.open(os.path.join(dir_path, "workloads", "write.properties"), "r") as inf:
    for line in inf:
        if line.startswith("db.url="):
            line = line.replace("db.url=", "").replace("\r", "").replace("\n", "")
            query_url = line
        if line.startswith("db.feedport="):
            line = line.replace("db.feedport=", "").replace("\r", "").replace("\n", "")
            feed_port = int(line)
inf.close()


def exe_sqlpp(cmd):
    r = requests.post(query_url, data={"statement": cmd})
    if r.status_code == 200:
        return True
    else:
        print("Error: " + r.reason + "" + cmd)
        return False


def get_records():
    cmd = "USE ycsb; SELECT COUNT(*) AS cnt FROM usertable;"
    r = requests.post(query_url, data={"statement": cmd})
    if r.status_code == 200:
        for line in r.content.decode("utf-8").split("\n"):
            line = line.strip().replace("\r", "").replace(" ", "")
            if line.startswith("\"results\":[{\"cnt\":"):
                line = line.replace("\"results\":[{\"cnt\":", "").replace("}", "")
                return int(line)
        return 0
    else:
        print("Error: " + r.reason + "" + cmd)
        return -1


def create_dataverse():
    cmd = """DROP DATAVERSE ycsb IF EXISTS;
CREATE DATAVERSE ycsb;"""
    return exe_sqlpp(cmd)


def create_type():
    cmd = """USE ycsb;
CREATE TYPE usertype AS CLOSED {
    YCSB_KEY: string,
    field0: binary,
    field1: binary,
    field2: binary,
    field3: binary,
    field4: binary,
    field5: binary,
    field6: binary,
    field7: binary,
    field8: binary,
    field9: binary
};"""
    return exe_sqlpp(cmd)


def paras_to_str(paras):
    ret = []
    for k in sorted(paras.keys()):
        v = paras[k]
        if type(v) == str:
            ret.append("\"" + k + "\":\"" + v + "\"")
        else:
            ret.append("\"" + k + "\":" + str(v))
    return ",".join(ret)


def create_table():
    cmd = """USE ycsb;
CREATE DATASET usertable (usertype)
    PRIMARY KEY YCSB_KEY
    WITH {
        "merge-policy":{
            "name":"slow",
            "parameters":{
                "num-components":16,
                "min-components":4,
                "min-delay":4000,
                "max-delay":180000
            }
        }
    };"""
    return exe_sqlpp(cmd)


# Modify feed configuration here
def create_feed():
    cmd = """USE ycsb;
CREATE FEED userfeed WITH {
    "adapter-name":"socket_adapter",
    "sockets":"localhost:""" + str(feed_port) + """",
    "address-type":"IP",
    "type-name":"usertype",
    "format":"adm",
    "upsert-feed":"true"
};
CONNECT FEED userfeed TO DATASET usertable;"""
    return exe_sqlpp(cmd)


def start_feed():
    cmd = """USE ycsb;
START FEED userfeed;"""
    return exe_sqlpp(cmd)


def stop_feed():
    cmd = """USE ycsb;
STOP FEED userfeed;"""
    return exe_sqlpp(cmd)


def get_base_name(f):
    fn = os.path.basename(f)
    if "." in fn:
        tmp = fn.split(".")
        return ".".join(tmp[:-1])
    else:
        return fn


def wait_merge(k):
    print("Waiting for merge to complete (k=" + str(k) + ")...")
    while True:
        cnt = len(glob.glob(os.path.join(asterixdb, "data", "red", "storage", "partition_0", "ycsb", "usertable",
                                         "0", "usertable", "*_b")))
        if cnt <= k:
            return
        else:
            time.sleep(5)


def zip_log(zip_path, file_path):
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(file_path, os.path.basename(file_path), zipfile.ZIP_DEFLATED)
    z.close()
    try:
        os.remove(file_path)
    except:
        pass


def grep_logs():
    propsn = "props_slow.txt"
    flushn = "flushes_slow.csv"
    mergen = "merges_slow.csv"
    readn = "reads_slow.csv"

    propsf = open(os.path.join(logs_dir, propsn), "w")
    flushf = open(os.path.join(logs_dir, flushn), "w")
    mergef = open(os.path.join(logs_dir, mergen), "w")
    readf = open(os.path.join(logs_dir, readn), "w")

    for logp in glob.glob(os.path.join(asterixdb, "logs", "*.gz")):
        with gzip.open(logp, "rt") as inf:
            for line in inf:
                if "[PROPERTIES]" in line:
                    line = line[line.index("[PROPERTIES]")+13:].replace("\r", "")
                    propsf.write(line.replace(":", ": ").replace(",", "\n") + "\n")
                if "[FLUSH]" in line:
                    flushf.write(line[line.index("[FLUSH]")+8:].replace("\r", ""))
                if "[MERGE]" in line:
                    mergef.write(line[line.index("[MERGE]")+8:].replace("\r", ""))
                if "[SEARCH]" in line:
                    readf.write(line[line.index("[SEARCH]")+9:].replace("\r", ""))
        inf.close()
        try:
            os.remove(logp)
        except:
            pass

    for logp in glob.glob(os.path.join(asterixdb, "logs", "*.log")):
        with io.open(logp, "r", encoding="utf-8") as inf:
            for line in inf:
                if "[PROPERTIES]" in line:
                    line = line[line.index("[PROPERTIES]")+13:].replace("\r", "")
                    propsf.write(line.replace(":", ": ").replace(",", "\n") + "\n")
                if "[FLUSH]" in line:
                    flushf.write(line[line.index("[FLUSH]")+8:].replace("\r", ""))
                if "[MERGE]" in line:
                    mergef.write(line[line.index("[MERGE]")+8:].replace("\r", ""))
                if "[SEARCH]" in line:
                    readf.write(line[line.index("[SEARCH]")+9:].replace("\r", ""))
        inf.close()
        emptyf = open(logp, "w")
        emptyf.close()

    propsf.close()
    flushf.close()
    mergef.close()
    readf.close()


def reset():
    call("rm -fr \"" + asterixdb + "\"/logs", shell=True)
    call("rm -fr \"" + asterixdb + "\"/data", shell=True)


def run_exp():
    print("Started")

    # Stop server if necessary
    stop_server()

    reset()

    # Start server
    start_server()

    if not create_dataverse():
        print("Failed to create dataverse")
        return False
    print("Created dataverse")
    if not create_type():
        print("Failed to create type")
        return False
    print("Created type")
    if not create_table():
        print("Failed to create table")
        return False
    print("Created table")
    if not create_feed():
        print("Failed to create feed")
        return False
    if not start_feed():
        print("Failed to start feed")
        return False
    print("Feed started")

    propsn = "props_slow.txt"
    flushn = "flushes_slow.csv"
    mergen = "merges_slow.csv"
    readn = "reads_slow.csv"

    for n in (propsn, flushn, mergen, readn):
        try:
            os.remove(os.path.join(logs_dir, n))
        except:
            pass

    p_write = mp.Process(target=ycsb_job, args=(True, "write", 4, 0))
    p_read = mp.Process(target=ycsb_job, args=(False, "read", 1, 0))
    p_list = mp.Process(target=list_files, args=(10,))

    p_write.start()
    time.sleep(10)
    p_list.start()
    p_read.start()

    p_write.join()
    p_read.join()

    p_list.terminate()

    grep_logs()

    zip_log(os.path.join(logs_dir, flushn + ".zip"), os.path.join(logs_dir, flushn))
    zip_log(os.path.join(logs_dir, mergen + ".zip"), os.path.join(logs_dir, mergen))
    zip_log(os.path.join(logs_dir, readn + ".zip"), os.path.join(logs_dir, readn))

    stop_server()

    print("Done")


if __name__ == "__main__":
    run_exp()
