#!/usr/bin/env bash

SLAVES="scai02.cs.ucla.edu scai03.cs.ucla.edu scai04.cs.ucla.edu scai05.cs.ucla.edu scai06.cs.ucla.edu scai07.cs.ucla.edu scai08.cs.ucla.edu scai09.cs.ucla.edu scai10.cs.ucla.edu scai11.cs.ucla.edu scai12.cs.ucla.edu scai13.cs.ucla.edu scai14.cs.ucla.edu scai15.cs.ucla.edu scai16.cs.ucla.edu"
SPARK_HOME="/home/clash/sparks/spark-kddlog"

declare -a ARRAY

for var in $SLAVES
do
  echo "[Main] Pushing Bigdatalog2 confs and jars to $var"
  rsync -avhurR --inplace bin clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  rsync -avhurR --inplace sbin clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  # rsync -avhurR --inplace logs clash@$var:$SPARK_HOME/ &
  # ARRAY+=( $! )
  rsync -avhurR --inplace assembly/target/scala-2.11 clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  rsync -avhurR --inplace examples/target/scala-2.11 clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  rsync -avhurR --inplace datalog/target/scala-2.11 clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  rsync -avhurR --inplace conf/log4j.properties clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  rsync -avhurR --inplace conf/spark-env.sh clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
  rsync -avhurR --inplace conf/spark-defaults.conf clash@$var:$SPARK_HOME/ &
  ARRAY+=( $! )
done

for val in ""${ARRAY[@]}""
do
  echo "Wating on $val"
  wait ${val}
done
