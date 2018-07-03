#!/usr/bin/python

import os
import requests
import sys
from subprocess import call

### Modify the below parameters ###

# You may need to modify feed configuration in create_feed() if you don't use localhost

interpreter = "python3.5"  # Python interpreter to run YCSB

load_name = "load.properties"

# The tasks you want to run, in order
run_names = ("read_only.properties", "read_most.properties", "read_insert.properties")

load_threads = 4  # Use 4 threads for loading
run_threads = 1  # Use 1 thread for running workload

# Merge policies you are testing, key is the file name for log, value is the policy name and
# parameters for creating the dataset
policies = {
    # No merge policy
    "no-merge": (
        "no-merge",
        None
    ),
    # Constant policy, K=5
    "const-5": (
        "constant",
        {
            "num-components": 5
        }
    ),
    # Prefix policy, 4 components, max size 128MB
    "prefix-4-128MB": (
        "prefix",
        {
            "max-tolerance-component-count": 4,
            "max-mergable-component-size": 134217728,
        }
    ),
}

# Path of AsterixDB on server
asterixdb = "~/asterixdb/opt/local"


# You may comment the 2 call function for start_server and stop_server if you don't want restart the database

def start_server():
    # Use this if the database is on another machine
    # call("ssh REMOTE_SERVER \"bash -l -c '" + asterixdb + "/bin/start-sample-cluster.sh'\"", shell=True)

    # Run database on local machine
    call("\"" + asterixdb + "/bin/start-sample-cluster.sh\"", shell=True)

    pass


def stop_server():
    # Use this if the database is on another machine
    # call("ssh REMOTE_SERVER \"bash -l -c '" + asterixdb + "/bin/stop-sample-cluster.sh'\"", shell=True)

    # Run database on local machine
    call("\"" + asterixdb + "/bin/stop-sample-cluster.sh\"", shell=True)

    pass


### Modify the above parameters ###


dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

ycsb = os.path.join(dir_path, "ycsb-0.14.0-SNAPSHOT", "bin", "ycsb")
logs_dir = os.path.join(dir_path, "ycsb_logs")
if not os.path.isdir(logs_dir):
    os.mkdir(logs_dir)

load_path = os.path.join(dir_path, "workloads", load_name)
run_paths = [os.path.join(dir_path, "workloads", n) for n in run_names]

query_url = ""
feed_port = 0

with open(load_path, "r") as inf:
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


def create_table(name, paras):
    if paras is not None:
        cmd = """USE ycsb;
CREATE DATASET usertable (usertype)
    PRIMARY KEY YCSB_KEY
    WITH {
        "merge-policy":{
            "name":""" + "\"" + name + "\"" + """,
            "parameters":{""" + paras_to_str(paras) + """}
        }
    };"""
    else:
        cmd = """USE ycsb;
CREATE DATASET usertable (usertype)
    PRIMARY KEY YCSB_KEY
    WITH {
        "merge-policy":{
            "name":""" + "\"" + name + "\"" + """
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


def load(filename):
    base = get_base_name(load_name)
    if load_threads == 1:
        cmd = interpreter + " \"" + ycsb + "\" load asterixdb -P \"" + load_path + "\" -p exportfile=\"" + \
              os.path.join(logs_dir, filename + "." + base + ".txt") + "\" -s > \"" + \
              os.path.join(logs_dir, filename + "." + base + ".log") + "\""
    else:
        cmd = interpreter + " \"" + ycsb + "\" load asterixdb -P \"" + load_path + "\" -p exportfile=\"" + \
              os.path.join(logs_dir, filename + "." + base + ".txt") + "\" -s -threads " + str(load_threads) + \
              " > \"" + os.path.join(logs_dir, filename + "." + base + ".log") + "\""
    call(cmd, shell=True)


def run(filename, run_path):
    base = get_base_name(run_path)
    if run_threads == 1:
        cmd = interpreter + " \"" + ycsb + "\" run asterixdb -P \"" + run_path + "\" -p exportfile=\"" + \
              os.path.join(logs_dir, filename + "." + base + ".txt") + "\" -s > \"" + \
              os.path.join(logs_dir, filename + "." + base + ".log") + "\""
    else:
        cmd = interpreter + " \"" + ycsb + "\" run asterixdb -P \"" + run_path + "\" -p exportfile=\"" + \
              os.path.join(logs_dir, filename + "." + base + ".txt") + "\" -s -threads " + str(run_threads) + \
              " > \"" + os.path.join(logs_dir, filename + "." + base + ".log") + "\""
    call(cmd, shell=True)


def run_exp(filename, policy):
    print("Start " + filename)

    # Stop server if necessary
    stop_server()

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
    if not create_table(policy[0], policy[1]):
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

    load(filename)

    for run_path in run_paths:
        # Stop server if necessary
        stop_server()

        # Start server
        start_server()

        # Start feed in case it's stopped
        try:
            start_feed()
        except BaseException:
            pass

        base = get_base_name(run_path)
        print("Run " + base)
        run(filename, run_path)
        print("Finished run " + base)

    print("Done " + filename)


if __name__ == "__main__":
    for filename, policy in policies.items():
        run_exp(filename, policy)
