#!/usr/bin/env bash
source PerfRun.conf

#Stop all
cd $SnappyData
./sbin/snappy-stop-all.sh

#Clean up the work directory and conf settings
rm -rf work/
rm -rf conf/servers
rm -rf conf/leads
rm -rf conf/locators
