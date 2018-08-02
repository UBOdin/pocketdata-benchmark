#!/bin/bash
# run benchmark
adb -s $1 shell pm disable com.example.benchmark_withjson
sleep 1
adb -s $1 shell pm enable com.example.benchmark_withjson

printf "Rebooting and running benchmark on device %s\n" $1

adb -s $1 reboot
adb -s $1 wait-for-device
sleep 40
printf "Rebooted\n"
adb root
sub=""
while [ "$sub" != "uid=0(" ]; do
	sleep 5
	var=$(adb shell id)
	sub=${var%%root*}  # var:4:1
	printf ".\n"
done
sleep 40
printf "Rooted\n"

adb -s $1 shell sh /data/removeBenchmarkData.sh
adb -s $1 shell sh /data/preBenchmark.sh $2 $3 $4 $5 #create database
adb -s $1 shell pm disable com.example.benchmark_withjson
sleep 5
adb -s $1 shell pm enable com.example.benchmark_withjson
adb -s $1 shell sh /data/benchmark.sh $4 #run queries -- specify governor ($4)

# Get results (and trap for error):
result="$(adb shell cat /data/results.txt)"
#error=${result:0:3}
#if [ ${error} == "ERR" ]; then
#	echo "Error on phone:  $result"
#	exit 1
#fi

# pull log
adb -s $1 pull /data/trace.log
# $2 = DB (sql, bdb, bdb100); $3 = workload (A, B, C etc.); $5 = delay (lognormal etc.)
filename="YCSB_${2}_${3}_${5}.log"
mv trace.log logs/$filename
gzip logs/$filename
sleep 1
printf "FILENAME:  %s\n" "$filename"

echo "RESULTS: $result"

printf "Completed benchmark for device %s\n" $1

