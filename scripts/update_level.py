#!/usr/local/bin/python3.7

import os
import glob
import requests
import sys
import time
import zipfile
import gzip
from subprocess import call
from operator import itemgetter


if len(sys.argv) != 3:
    print("Usage: {0} SIZE_RATIO DIST".format(os.path.basename(os.path.realpath(__file__))))
    sys.exit(-1)
SIZE_RATIO = int(sys.argv[1])
print("Size ratio {0}".format(SIZE_RATIO))
dist = str(sys.argv[2])
print("Distribution " + dist)

load_name = "update.properties"
load_threads = 1  # Use single thread for loading to guarantee ordered insertion


root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
ycsb = os.path.join(root, "ycsb-asterixdb-binding-0.18.0-SNAPSHOT", "bin", "ycsb")
load_path = os.path.join(root, "workloads", load_name)

logs_dir = os.path.join(root, "logs")
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)

# Path of AsterixDB on server
asterixdb = os.path.join(root, "asterixdb", "opt", "local")


asterixdb_logs = os.path.join(asterixdb, "logs")
usertable_dir = os.path.join(asterixdb, "data", "red", "storage", "partition_0", "ycsb", "usertable", "0", "usertable")
if not os.path.islink(os.path.join(root, "asterixdb_logs")):
    os.symlink(asterixdb_logs, os.path.join(root, "asterixdb_logs"))
if not os.path.islink(os.path.join(root, "usertable")):
    os.symlink(usertable_dir, os.path.join(root, "usertable"))


def start_server():
    call("bash \"" + os.path.join(asterixdb, "bin", "start-sample-cluster.sh") + "\"", shell=True)


def stop_server():
    call("bash \"" + os.path.join(asterixdb, "bin", "stop-sample-cluster.sh") + "\"", shell=True)


query_url = ""
feed_port = 0
insertorder = ""
with open(load_path, "r") as inf:
    for line in inf:
        if line.startswith("db.url="):
            line = line.replace("db.url=", "").replace("\r", "").replace("\n", "")
            query_url = line
        if line.startswith("db.feedport="):
            line = line.replace("db.feedport=", "").replace("\r", "").replace("\n", "")
            feed_port = int(line)
        if line.startswith("insertorder="):
            line = line.replace("insertorder=", "").replace("\r", "").replace("\n", "")
            insertorder = line
inf.close()

task_name = "level_" + insertorder + "_" + str(SIZE_RATIO) + "_" + dist

print("Num load threads: " + str(load_threads))
print("Insert order: " + insertorder)
print("Task name: " + task_name)


def write_err(msg):
    err_log = open(os.path.join(logs_dir, task_name + ".err"), "a")
    if msg.endswith("\n"):
        err_log.write(msg)
    else:
        err_log.write(msg + "\n")
    err_log.close()


def exe_sqlpp(cmd):
    while "  " in cmd:
        cmd = cmd.replace("  ", " ")
    r = requests.post(query_url, data={"statement": cmd})
    if r.status_code == 200:
        return True
    else:
        print("Error: " + r.reason + ": " + cmd)
        write_err("Error: " + cmd + ": " + r.text)
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
        print("Error: " + r.reason + " " + cmd)
        write_err("Error: " + cmd + ": " + r.text)
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
    cmd = """USE ycsb;
CREATE DATASET usertable (usertype)
    PRIMARY KEY YCSB_KEY
    WITH {
        "merge-policy":{
            "name":"level",
            "parameters":{
                "num-components-0":2,
                "num-components-1":""" + str(SIZE_RATIO) + """,
                "pick":"min-overlap"
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


def load():
    if load_threads == 1:
        thread_str = ""
    else:
        thread_str = " -threads " + str(load_threads)
    record_str = " -p insertstart=0"
    dist_str = " -p keydistribution=" + dist
    cmd = "python3.7 \"" + ycsb + "\" load asterixdb -P \"" + load_path + "\"" + record_str + dist_str +\
        " -s" + thread_str + " -p exportfile=\"" + os.path.join(logs_dir, task_name + ".ycsb.load.log") + "\""
    call(cmd, shell=True)


def reset():
    call("rm -fr \"" + asterixdb_logs + "\"", shell=True)
    call("rm -fr \"" + asterixdb + "\"/data", shell=True)


flush_flag = os.path.join(usertable_dir, "is_flushing")
merge_flag = os.path.join(usertable_dir, "is_merging")


def wait_io():
    while os.path.isfile(flush_flag) or os.path.isfile(merge_flag):
        print("I/O pending...")
        time.sleep(10)


def parseline(line, kw):
    if kw in line:
        return line[line.index(kw) + len(kw) + 1:].replace("\n", "").split("\t")
    else:
        return []


def count_levels(components):
    lvs = set()
    for c in components:
        lvs.add(int(c.split("_")[0]))
    return len(lvs)


def extract_load_logs():
    with open(os.path.join(logs_dir, task_name + ".load.tmp1"), "w") as loadtmpf, \
            open(os.path.join(logs_dir, task_name + ".tables.tmp"), "w") as tablestmpf:
        for logp in glob.glob(os.path.join(asterixdb, "logs", "nc-red*")):
            with (open(logp, "r") if logp.endswith(".log") else gzip.open(logp, "rt")) as logf:
                is_err = False
                for line in logf:
                    if is_err:
                        if " WARN " in line or " INFO " in line or "\tWARN\t" in line or "\tINFO\t" in line:
                            is_err = False
                    else:
                        if " ERROR " in line or "\tERROR\t" in line:
                            is_err = True
                    if not is_err:
                        if "[FLUSH]\tusertable" in line:
                            parts = parseline(line, "[FLUSH]\tusertable")
                            if len(parts) != 7:
                                continue
                            timestamp = parts[0]
                            new_component = parts[6]
                            c_name = new_component.split(":")[0].replace("_", "\t")
                            loadtmpf.write(timestamp + "\tF\t" + "\t".join(parts[1:]) + "\n")
                            tablestmpf.write(c_name + "\t" + new_component.replace(":", "\t") + "\n")
                        elif "[MERGE]\tusertable" in line:
                            parts = parseline(line, "[MERGE]\tusertable")
                            if len(parts) != 8:
                                continue
                            timestamp = parts[0]
                            new_components = parts[7].split(";")
                            loadtmpf.write(timestamp + "\tM\t" + "\t".join(parts[1:]) + "\n")
                            for new_component in new_components:
                                c_name = new_component.split(":")[0].replace("_", "\t")
                                tablestmpf.write(c_name + "\t" + new_component.replace(":", "\t") + "\n")
                        elif "[COMPONENTS]\tusertable" in line:
                            parts = parseline(line, "[COMPONENTS]\tusertable")
                            if len(parts) != 2:
                                continue
                            loadtmpf.write(parts[0] + "\tC\t" + parts[1] + "\n")
                        else:
                            continue
            logf.close()
    loadtmpf.close()
    tablestmpf.close()
    call("sort -n -k1,1 \"{0}.tmp1\" |  cut -f2- > \"{0}.tmp2\""
         .format(os.path.join(logs_dir, task_name + ".load")), shell=True)
    try:
        os.remove(os.path.join(logs_dir, task_name + ".load.tmp1"))
    except:
        pass
    call("sort -n -k1,1 -k2,2 \"{0}.tmp\" |  cut -f3- > \"{0}.log\""
         .format(os.path.join(logs_dir, task_name + ".tables")), shell=True)
    try:
        os.remove(os.path.join(logs_dir, task_name + ".tables.tmp"))
    except:
        pass
    total_flushed = []
    total_merged = []
    tmp_space = []
    num_levels = []
    with open(os.path.join(logs_dir, task_name + ".load.tmp2"), "r") as inf:
        for line in inf:
            parts = line.replace("\n", "").split("\t")
            if len(parts) < 2:
                continue
            if parts[0] == "F":
                flushed = int(parts[3])
                merged = int(parts[4])
                total_flushed.append(flushed)
                total_merged.append(merged)
                tmp_space.append(0)
                if len(num_levels) == 0:
                    num_levels.append(1)
                else:
                    num_levels.append(num_levels[-1])
            elif parts[0] == "M":
                f_cnt = int(parts[1])
                merged = int(parts[4])
                new_components = parts[7].split(";")
                new_size = 0
                for c in new_components:
                    new_size += int(c.split(":")[1])
                total_merged[f_cnt - 1] = merged
                tmp_space[f_cnt - 1] = max(tmp_space[f_cnt - 1], new_size)
            elif parts[0] == "C":
                components = parts[1].split(";")
                f_cnt = int(components[0].split("_")[1])
                if f_cnt > len(num_levels):
                    num_levels.append(count_levels(components))
                else:
                    num_levels[f_cnt - 1] = count_levels(components)
            else:
                continue
    inf.close()
    try:
        os.remove(os.path.join(logs_dir, task_name + ".load.tmp2"))
    except:
        pass
    with open(os.path.join(logs_dir, task_name + ".load.log"), "w") as outf:
        for i in range(len(total_merged)):
            outf.write("{0}\t{1}\t{2}\t{3}\n".format(total_flushed[i], total_merged[i], num_levels[i], tmp_space[i]))
    outf.close()


def zip_logs():
    in_files = [
        os.path.join(logs_dir, task_name + ".tables.log"),
        os.path.join(logs_dir, task_name + ".load.log"),
        os.path.join(logs_dir, task_name + ".ycsb.load.log"),
        os.path.join(logs_dir, task_name + ".err")
    ]
    with zipfile.ZipFile(os.path.join(logs_dir, task_name + ".zip"), "w") as z:
        for f in in_files:
            if os.path.isfile(f) and os.path.getsize(f) > 0:
                z.write(f, os.path.basename(f), zipfile.ZIP_DEFLATED)
    z.close()
    for f in in_files:
        if os.path.isfile(f):
            os.remove(f)


def run_taks():
    print("Started")

    # Stop server if necessary
    stop_server()

    reset()

    # Start server
    start_server()

    if not create_dataverse():
        print("Failed to create dataverse", file=sys.stderr)
        return False
    print("Created dataverse")
    if not create_type():
        print("Failed to create type", file=sys.stderr)
        return False
    print("Created type")
    if not create_table():
        print("Failed to create table", file=sys.stderr)
        return False
    print("Created table")
    if not create_feed():
        print("Failed to create feed", file=sys.stderr)
        return False
    if not start_feed():
        print("Failed to start feed", file=sys.stderr)
        return False
    print("Feed started")

    load()
    wait_io()
    extract_load_logs()
    zip_logs()

    stop_feed()

    stop_server()

    print("Done")


if __name__ == "__main__":
    run_taks()
