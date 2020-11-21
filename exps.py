#!/usr/bin/env python

# This script will iterate over different combination of arg values,
# and conduct each exp in repeats.
#
# Note the order of arg and its values (split by '/') matter.

# Programs:
#   11: TC-LL
#   21: SG
#   31: APSP
#   32: SSSP
#   41: CC
#   51: Reach/BFS
#   52: Reach CountPaths
#   53: CountPaths
#   71: Triangle Counting

import subprocess

repeats = 5

expArgs = [
    "program = 51",
    "startvertex = 234580/39331/573030/707092/887365",
    "codegen = false",
    "input = rmat-1M.txt/rmat-2M.txt/rmat-4M.txt/rmat-8M.txt/rmat-16M.txt/rmat-32M.txt/rmat-64M.txt/rmat-128M.txt",
    #
    "aggrIterType = prim", # other values: tungsten
    "pinRDDHostLimit = 15",
    "partitions = 120" # pined * 8
    # "packedBroadcast = true"
]

hdfsDir = "hdfs://scai01.cs.ucla.edu:9000/user/"
output = "" # " -output=exps/output.txt" # if collect result
script = "./run.sh"



def execute(cmd, remainArgs):
    if (remainArgs):
        key, values = remainArgs[0].split("=")
        key = key.strip()
        vPrefix = hdfsDir if (key == "input") else ""
        for v in values.split("/"):
            opt = " -" + key + "=" + vPrefix + v.strip()
            execute(cmd + opt, remainArgs[1:])
    else:
        cmd += output
        for i in range(repeats):
            print "[Main] Execute: " + cmd
            # execute cmd
            process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            out, error = process.communicate()
            print out

execute(script, expArgs)


