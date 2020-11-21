#!/usr/bin/env bash

# ./stop.sh

./sbin/start-all.sh
sleep 1
./sbin/start-history-server.sh
