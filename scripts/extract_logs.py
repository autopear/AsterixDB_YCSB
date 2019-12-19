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


if len(sys.argv) != 2:
    print("Usage: {0} TASK_NAME".format(os.path.basename(os.path.realpath(__file__))))
    sys.exit(-1)
task_name = str(sys.argv[1])


root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

logs_dir = os.path.join(root, "logs")
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)

# Path of AsterixDB on server
asterixdb = os.path.join(root, "asterixdb", "opt", "local")


asterixdb_logs = os.path.join(asterixdb, "logs")
usertable_dir = os.path.join(asterixdb, "data", "red", "storage", "partition_0", "ycsb", "usertable", "0", "usertable")

flush_flag = os.path.join(usertable_dir, "is_flushing")
merge_flag = os.path.join(usertable_dir, "is_merging")


def wait_io():
    while os.path.isfile(flush_flag) or os.path.isfile(merge_flag):
        print("I/O pending...")
        time.sleep(10)


def extract_load_logs():
    last_stats = None
    with open(os.path.join(logs_dir, task_name + ".load.tmp"), "w") as tmpf:
        for logp in glob.glob(os.path.join(asterixdb, "logs", "*.log")):
            with open(logp, "r") as logf:
                is_err = False
                for line in logf:
                    if is_err:
                        if " WARN " in line or " INFO " in line or "\tWARN\t" in line or "\tINFO\t" in line:
                            is_err = False
                        else:
                            write_err(line)
                    else:
                        if " ERROR " in line or "\tERROR\t" in line:
                            is_err = True
                            write_err(line)
                    if not is_err:
                        if "[MERGE]" in line:
                            tmp = line.split("\t")
                            flush = -1
                            merge = -1
                            for kv in tmp:
                                if kv.startswith("flushes="):
                                    flush = int(kv.replace("flushes=", ""))
                                if kv.startswith("merges="):
                                    merge = int(kv.replace("merges=", ""))
                            tmpf.write("{0}\t{1}\t{2}\n".format(flush, merge, line[line.index("[MERGE]") + 8:]
                                                                .replace("\r", "").replace("\n", "")))
                        if "[ALL]" in line:
                            tmp = line[line.index("[ALL]") + 6:]\
                                .replace("\r", "").replace("\n", "").split("\t")
                            fcnt = int(tmp[0])
                            mcnt = int(tmp[1])
                            cinfo = tmp[2]
                            if (last_stats is None) or \
                                    (fcnt > last_stats[0]) or \
                                    (fcnt == last_stats[0] and mcnt > last_stats[1]):
                                last_stats = (fcnt, mcnt, cinfo)
            logf.close()
        for logp in glob.glob(os.path.join(asterixdb, "logs", "*.gz")):
            with gzip.open(logp, "rt") as logf:
                is_err = False
                for line in logf:
                    if is_err:
                        if " WARN " in line or " INFO " in line or "\tWARN\t" in line or "\tINFO\t" in line:
                            is_err = False
                        else:
                            write_err(line)
                    else:
                        if " ERROR " in line or "\tERROR\t" in line:
                            is_err = True
                            write_err(line)
                    if not is_err:
                        if "[MERGE]" in line:
                            tmp = line.split("\t")
                            flush = -1
                            merge = -1
                            for kv in tmp:
                                if kv.startswith("flushes="):
                                    flush = int(kv.replace("flushes=", ""))
                                if kv.startswith("merges="):
                                    merge = int(kv.replace("merges=", ""))
                            tmpf.write("{0}\t{1}\t{2}\n".format(flush, merge, line[line.index("[MERGE]") + 8:]
                                                                .replace("\r", "").replace("\n", "")))
                        if "[ALL]" in line:
                            tmp = line[line.index("[ALL]") + 6:]\
                                .replace("\r", "").replace("\n", "").split("\t")
                            fcnt = int(tmp[0])
                            mcnt = int(tmp[1])
                            cinfo = tmp[2]
                            if (last_stats is None) or \
                                    (fcnt > last_stats[0]) or \
                                    (fcnt == last_stats[0] and mcnt > last_stats[1]):
                                last_stats = (fcnt, mcnt, cinfo)
            logf.close()
    tmpf.close()
    call("sort -n -k1,1 -k2,2 \"{0}.tmp\" |  cut -f3- > \"{0}.log\""
         .format(os.path.join(logs_dir, task_name + ".load")), shell=True)
    try:
        os.remove(os.path.join(logs_dir, task_name + ".load.tmp"))
    except:
        pass
    with open(os.path.join(logs_dir, task_name + ".tables.log"), "w") as outf:
        if last_stats is not None:
            outf.write("{0}\t{1}\t{2}".format(last_stats[0], last_stats[1], last_stats[2]))
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


if __name__ == "__main__":
    wait_io()
    extract_load_logs()
    zip_logs()