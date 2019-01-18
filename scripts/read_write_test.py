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
import threading


interpreter = "python3.7"  # Python interpreter to run YCSB

dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

asterixdb = os.path.join(dir_path, "asterix-server", "opt", "local")
ycsb = os.path.join(dir_path, "ycsb-asterixdb-binding-0.16.0-SNAPSHOT", "bin", "ycsb")
logs_dir = os.path.join(dir_path, "logs")
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)


class YCSB(threading.Thread):
    def __init__(self, k, workload, num_threads):
        threading.Thread.__init__(self)
        self.k = k
        self.name = workload
        self.num_threads = num_threads
        self.workload = os.path.join(dir_path, "workloads", workload + ".properties")

    def run(self):
        if self.num_threads == 1:
            thread_str = ""
        else:
            thread_str = " -threads " + str(self.num_threads)
        basename = self.name + "_" + str(self.k) + "_"
        cmd = interpreter + " \"" + ycsb + "\" run asterixdb -P \"" + self.workload + "\"" + \
              " -p exportfile=\"" + os.path.join(logs_dir, basename + "final.log") + \
              "\" -s" + thread_str + " > \"" + os.path.join(logs_dir, basename + "realtime.log") + "\""
        call(cmd, shell=True)


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


def create_table(k):
    cmd = """USE ycsb;
CREATE DATASET usertable (usertype)
    PRIMARY KEY YCSB_KEY
    WITH {
        "merge-policy":{
            "name":"binomial",
            "parameters":{
                "num-components":""" + str(k) + """
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
    os.remove(file_path)


def grep_logs(k):
    flushn = "flushes_" + str(k) + ".csv"
    mergen = "merges_" + str(k) + ".csv"
    readn = "reads_" + str(k) + ".csv"

    flushf = open(os.path.join(logs_dir, flushn), "a")
    mergef = open(os.path.join(logs_dir, mergen), "a")
    readf = open(os.path.join(logs_dir, readn), "a")

    for logp in glob.glob(os.path.join(asterixdb, "logs", "*.gz")):
        with gzip.open(logp, "rt") as inf:
            for line in inf:
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
                if "[FLUSH]" in line:
                    flushf.write(line[line.index("[FLUSH]")+8:].replace("\r", ""))
                if "[MERGE]" in line:
                    mergef.write(line[line.index("[MERGE]")+8:].replace("\r", ""))
                if "[SEARCH]" in line:
                    readf.write(line[line.index("[SEARCH]")+9:].replace("\r", ""))
        inf.close()
        emptyf = open(logp, "w")
        emptyf.close()

    flushf.close()
    mergef.close()
    readf.close()


def reset():
    call("rm -fr \"" + asterixdb + "\"/logs", shell=True)
    call("rm -fr \"" + asterixdb + "\"/data", shell=True)


def run_exp(k):
    print("Started k=" + str(k))

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
    if not create_table(k):
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

    flushn = "flushes_" + str(k) + ".csv"
    mergen = "merges_" + str(k) + ".csv"
    readn = "reads_" + str(k) + ".csv"

    for n in (flushn, mergen, readn):
        try:
            os.remove(os.path.join(logs_dir, n))
        except:
            pass

    thread_write = YCSB(k, "write", 4)
    thread_read = YCSB(k, "read", 4)

    thread_write.start()
    thread_read.start()

    thread_write.join()
    thread_read.join()

    wait_merge(k)
    grep_logs(k)

    zip_log(os.path.join(logs_dir, flushn + ".zip"), os.path.join(logs_dir, flushn))
    zip_log(os.path.join(logs_dir, mergen + ".zip"), os.path.join(logs_dir, mergen))
    zip_log(os.path.join(logs_dir, readn + ".zip"), os.path.join(logs_dir, readn))

    stop_server()

    print("Done k=" + str(k))


if __name__ == "__main__":
    run_exp(8)
