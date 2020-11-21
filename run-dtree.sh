#!/bin/bash
DNAMES="real-sim rcv1_test.binary kdd10"
MAXITER="3"
ITERATIONS="3"
for dn in $DNAMES
do
    echo "#######################################################"
    echo "############ DTree: $dn ####################"
    echo "######################################################"
    for miter in $MAXITER
    do
        for iter in `seq 1 $ITERATIONS`
        do
            echo "######################################################"
            echo "DTree: $dn max_iteration: $miter iteration $iter"
            echo "######################################################"
            ./run.sh -log=error \
            -program=decision-tree \
            -partitions=120 \
            -codegen=true \
            -fixpointTask=false \
            -maxIterations=${miter} \
            -init=/home/clash/sparks/kddlog-data/dtree/${dn}/init.csv \
            -iset=/home/clash/sparks/kddlog-data/dtree/${dn}/iset.csv \
            -train=/home/clash/sparks/kddlog-data/dtree/${dn}/train.csv \
            -dec=/home/clash/sparks/kddlog-data/dtree/${dn}/decision.csv \
            -expand=/home/clash/sparks/kddlog-data/dtree/${dn}/expand.csv \
            -output=testdata/results/dtree.out
        done
    done
done
