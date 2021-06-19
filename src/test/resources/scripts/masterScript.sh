#!/usr/bin/env bash
source PerfRun.conf

#Start the cluster
cat > $SnappyData/conf/leads << EOF
$leads $bootStrapTrue
EOF
echo "******************Created conf/leads*********************"

cat > $SnappyData/conf/locators << EOF
$locator
EOF
echo "******************Created conf/locators******************"

for element in "${servers[@]}";
  do
        echo $element $serverMinMemory $serverMaxMemory $logLevel >> $SnappyData/conf/servers
  done
echo "******************Created conf/servers******************"


#cd $SnappyData
#./sbin/snappy-start-all.sh

sh $SnappyData/sbin/snappy-start-all.sh

#Status of cluster
#./sbin/snappy-status-all.sh

#cd $SnappyData
echo "****************** Starting Snappyjob ******************"
. $SnappyData/bin/snappy-job.sh submit --lead $leads:8090 --app-name myApp --class $className --app-jar $jarFile --conf dataLocation=$dataLocation --conf numIter=$numIter --conf warmup=$warmup --conf queryFile=$queryFile --conf sampleFraction=$sampleFraction --conf qcsParam1=$qcsParam1 --conf qcsParam2=$qcsParam2 --conf qcsParam3=$qcsParam3 --conf redundancy=$redundancy --conf perfTest=$perfTest --conf dropTable=$dropTable

