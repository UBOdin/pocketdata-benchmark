# setup phone

printf "Pushing files for device %s for DB cache $2\n" $1
 
adb -s $1 root
adb -s $1 push files/phone/benchmark.sh /data/benchmark.sh
adb -s $1 push files/phone/removeBenchmarkData.sh /data/removeBenchmarkData.sh
adb -s $1 push files/phone/preBenchmark.sh /data/preBenchmark.sh
adb -s $1 push files/phone/makepipe /data/makepipe  # mknod is MIA

if [ -n "$2" ]; then
	adb -s $1 push files/bdb/100MB/DB_CONFIG /data/
fi

