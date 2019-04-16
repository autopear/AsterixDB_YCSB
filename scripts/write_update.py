#!/usr/bin/python3

import os
import io
import glob
import requests
import sys
import time
import zipfile
import gzip
from subprocess import call

if len(sys.argv) != 4:
    print("Usage: {} policy k dist".format(os.path.basename(__file__)))
    sys.exit(-1)

interpreter = sys.executable

load_name = "load.properties"

load_threads = 2  # Use 4 threads for loading

dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

user_policy = str(sys.argv[1])
user_k = int(sys.argv[2])
user_dist = str(sys.argv[3])

if user_dist != "uniform" and user_dist != "zipfian" and user_dist != "latest" and user_dist != "binomial":
    print("Dist: uniform zipfian latest binomial")
    sys.exit(-1)

# Path of AsterixDB on server
asterixdb = os.path.join(dir_path, "asterix-server", "opt", "local")


def start_server():
    call("\"" + asterixdb + "/bin/start-sample-cluster.sh\"", shell=True)


def stop_server():
    call("\"" + asterixdb + "/bin/stop-sample-cluster.sh\" -f", shell=True)


def read_count(f):
    with open(f, "r") as inf:
        for line in inf:
            if line.startswith("insertcount="):
                line = line.replace("insertcount=", "").replace("\r", "").replace("\n", "").strip()
                c = int(line)
    inf.close()
    return c


def get_policy():
    if user_policy == "binomial":
        return """CREATE DATASET usertable (usertype)
PRIMARY KEY YCSB_KEY
WITH {
    "merge-policy":{
        "name":"binomial",
        "parameters":{
            "num-components":""" + str(user_k) + """
        }
    }
};"""
    elif user_policy == "min-latency":
        return """CREATE DATASET usertable (usertype)
PRIMARY KEY YCSB_KEY
WITH {
    "merge-policy":{
        "name":"min-latency",
        "parameters":{
            "num-components":""" + str(user_k) + """
        }
    }
};"""
    elif user_policy == "google-default":
        return """CREATE DATASET usertable (usertype)
PRIMARY KEY YCSB_KEY
WITH {
    "merge-policy":{
        "name":"google-default",
        "parameters":{
            "num-components":""" + str(user_k) + """
        }
    }
};"""
    elif user_policy == "exploring":
        if user_k <= 6:
            return """CREATE DATASET usertable (usertype)
PRIMARY KEY YCSB_KEY
WITH {
    "merge-policy":{
        "name":"exploring",
        "parameters":{
            "lambda":1.2,
            "min":2,
            "max":""" + str(user_k) + """
        }
    }
};"""
        else:
            return """CREATE DATASET usertable (usertype)
PRIMARY KEY YCSB_KEY
WITH {
    "merge-policy":{
        "name":"exploring",
        "parameters":{
            "lambda":1.2,
            "min":3,
            "max":""" + str(user_k) + """
        }
    }
};"""
    else:
        return ""


ycsb = os.path.join(dir_path, "ycsb-asterixdb-binding-0.16.0-SNAPSHOT", "bin", "ycsb")
logs_dir = os.path.join(dir_path, "logs")
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)

load_path = os.path.join(dir_path, "workloads", load_name)
load_cnt = read_count(load_path)

query_url = ""
feed_port = 0

with io.open(load_path, "r") as inf:
    for line in inf:
        if line.startswith("db.url="):
            line = line.replace("db.url=", "").replace("\r", "").replace("\n", "")
            query_url = line
        if line.startswith("db.feedport="):
            line = line.replace("db.feedport=", "").replace("\r", "").replace("\n", "")
            feed_port = int(line)
inf.close()


def parser_query(q):
    r = q.replace("\n", " ")
    while "  " in r:
        r = r.replace("  ", " ")
    return r


def exe_sqlpp(cmd):
    pcmd = parser_query(cmd)
    r = requests.post(query_url, data={"statement": pcmd})
    if r.status_code == 200:
        return True
    else:
        print("Error: " + r.reason + "" + pcmd)
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


def create_table():
    cmd = "USE ycsb; " + get_policy()
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


def load():
    if load_threads == 1:
        thread_str = ""
    else:
        thread_str = " -threads " + str(load_threads)
    dist_str = " -p keydistribution=" + user_dist
    cmd = interpreter + " \"" + ycsb + "\" load asterixdb -P \"" + load_path + "\"" + \
        dist_str + " -s" + thread_str
    call(cmd, shell=True)


def wait_merge():
    print("Waiting for merge to complete (k=" + str(user_k) + ")...")
    while True:
        cnt = len(glob.glob(os.path.join(asterixdb, "data", "red", "storage", "partition_0", "ycsb", "usertable",
                                         "0", "usertable", "*_b")))
        if cnt <= user_k:
            return
        else:
            time.sleep(5)


def zip_log(zip_path, file_path):
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(file_path, os.path.basename(file_path), zipfile.ZIP_DEFLATED)
    z.close()
    os.remove(file_path)


def grep_logs(records):
    flushn = user_policy + "_flushes_" + str(user_k) + "_" + user_dist + "_0.7.csv"
    mergen = user_policy + "_merges_" + str(user_k) + "_" + user_dist + "_0.7.csv"

    flushf = open(os.path.join(logs_dir, flushn), "a")
    mergef = open(os.path.join(logs_dir, mergen), "a")

    for logp in glob.glob(os.path.join(asterixdb, "logs", "*.gz")):
        with gzip.open(logp, "rt") as inf:
            for line in inf:
                if "[FLUSH]" in line:
                    flushf.write(str(records) + "," + line[line.index("[FLUSH]")+8:].replace("\r", ""))
                if "[MERGE]" in line:
                    mergef.write(str(records) + "," + line[line.index("[MERGE]")+8:].replace("\r", ""))
        inf.close()
        try:
            os.remove(logp)
        except:
            pass

    for logp in glob.glob(os.path.join(asterixdb, "logs", "*.log")):
        with io.open(logp, "r", encoding="utf-8") as inf:
            for line in inf:
                if "[FLUSH]" in line:
                    flushf.write(str(records) + "," + line[line.index("[FLUSH]")+8:].replace("\r", ""))
                if "[MERGE]" in line:
                    mergef.write(str(records) + "," + line[line.index("[MERGE]")+8:].replace("\r", ""))
        inf.close()
        emptyf = open(logp, "w")
        emptyf.close()

    flushf.close()
    mergef.close()


def reset():
    call("rm -fr \"" + asterixdb + "\"/logs", shell=True)
    call("rm -fr \"" + asterixdb + "\"/data", shell=True)


def run_exp():
    print("Started k=" + str(user_k) + " " + user_policy)

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

    flushn = user_policy + "_flushes_" + str(user_k) + "_" + user_dist + "_0.7.csv"
    mergen = user_policy + "_merges_" + str(user_k) + "_" + user_dist + "_0.7.csv"

    for n in (flushn, mergen):
        try:
            os.remove(os.path.join(logs_dir, n))
        except:
            pass

    records = 0

    startTime = time.time()

    print("Loading...")
    load()
    time.sleep(10)
    wait_merge()
    grep_logs(records)
    print("Load done")

    num_records = get_records()

    totalTime = time.time() - startTime

    try:
        outf = open(os.path.join(logs_dir, user_policy + "_" + str(user_k) + "_" + user_dist + "_0.7.log"), "w")
        outf.write("T: " + str(totalTime) + "\n")
        outf.write("R: " + str(num_records) + "\n")
        outf.close()
    except:
        pass

    zip_log(os.path.join(logs_dir, flushn + ".zip"), os.path.join(logs_dir, flushn))
    zip_log(os.path.join(logs_dir, mergen + ".zip"), os.path.join(logs_dir, mergen))

    stop_server()

    print("Done k=" + str(user_k) + " " + user_policy)


if __name__ == "__main__":
    run_exp()
