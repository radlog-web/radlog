#!/usr/bin/env bash

# ./stop.sh
# sleep 1

if [ -z "$1" ]; then
  build/sbt -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests package && ./rsync.sh
else
  echo "[Main] clean sync first" && ./clean_sync.sh && echo "[Main] build/sbt clean" && build/sbt -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests clean package && ./rsync.sh
fi

# sleep 1
# ./start.sh
