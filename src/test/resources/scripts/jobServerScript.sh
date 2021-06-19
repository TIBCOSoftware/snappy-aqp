#!/usr/bin/env bash
source PerfRun.conf

#cd $SnappyData
echo "****************** Starting Snappyjob ******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name myApp --class $className --app-jar $jarFile --conf dataLocation=$dataLocation --conf numIter=$numIter --conf warmup=$warmup --conf queryFile=$queryFile --conf sampleFraction=$sampleFraction --conf qcsParam1=$qcsParam1 --conf qcsParam2=$qcsParam2 --conf qcsParam3=$qcsParam3 --conf redundancy=$redundancy --conf perfTest=$perfTest --conf dropTable=$dropTable


