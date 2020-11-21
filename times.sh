#!/bin/bash

read -r -d '' usage << EOM
usage: run_times.sh <testtype> <times> <filename1> <filename2> ...

This util will run the given script using each file as input for running given times
EOM

if [ "$#" -lt 3 ]
  then
    echo "$usage"
    exit
fi

TYPE=$1
END=$2
for fname in "${@:3}"
do
  for i in $(seq 1 $END)
  do
    printf "[Main] Run $TYPE on $fname for $i times ...\n"
    cmd="./run_${TYPE}.sh $fname"
    (printf "[Command] $cmd\n" && $cmd) 2>&1
  done
done
