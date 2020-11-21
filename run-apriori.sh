#!/bin/bash
#SIZES="1 2 4 8 16 32 64 128 256"
SIZES="256"
#MINSUP="1000 1500 2000 2500 3000"
MINSUP="3000"
ITERATIONS="3"
for size in $SIZES
do
    echo "#######################################################"
    echo "############Frequent Itemset: $size ####################"
    echo "######################################################"
    for ms in $MINSUP
    do
        ms_count=`expr $ms \* $size`
        ms_percent=`expr $ms / 100`
        for iter in `seq 1 $ITERATIONS`
        do
            if [ $size == "1" ] || [ $size == "2" ]
            then
                num_partition=30
            elif [ $size == "4" ] || [ $size == "8" ]|| [ $size == "16" ]
            then
                num_partition=60
            elif [ $size == "32" ] || [ $size == "64" ]
            then
                num_partition=120
            elif [ $size == "128" ] || [ $size == "256" ]
            then
                num_partition=240
            else
                num_partition=240
            fi
            echo "######################################################"
            echo "Frequent Itemset size: $size num_partition: $num_partition min_support: $ms_count iteration $iter"
            echo "######################################################"
            #./run.sh -log=error -program=apriori-simple -partitions=${num_partition} -codegen=true -fixpointTask=false -mbsk=/home/clash/sparks/kddlog-data/freqpat/${size}/mbsk.csv -output=/home/clash/sparks/kddlog-data/freqpat/${size}/sec-${ms_percent} -MS=${ms_count}
            ./run.sh -log=error -program=apriori-multiple -partitions=${num_partition} -codegen=true -fixpointTask=false -mbsk=/home/clash/sparks/kddlog-data/freqpat/${size}/mbsk.csv -init=/home/clash/sparks/kddlog-data/freqpat/${size}/sec-${ms_percent}.tsv -sec=/home/clash/sparks/kddlog-data/freqpat/${size}/sec-${ms_percent}.tsv -output=testdata/results/apriori-multiple.out -MS=${ms_count}
        done
    done
done