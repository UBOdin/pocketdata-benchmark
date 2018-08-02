#!/bin/bash

# This widget extract summary I/O data (CPU runtime and total latency) from each tracefile.
# It batch processes all 24 tracefiles and outputs a CSV summary textfile (for Excel etc.).

# N.b. needs Bash string features.  Will not run under sh / dash.

# python process.py tracefiles/clean_0ms/YCSB_WorkloadIC_TimingAbdb100.log.gz

printf "Hello %s\n" "World"
printf "Running for batch:  $1\n"

#declare -a array1=("Foo" "Bar" "Bletch")
database_array=("sql" "bdb" "bdb100")
workload_array=("A" "B" "C" "D" "E" "F" "IB" "IC")
exit_code=0

for i in "${workload_array[@]}"; do
	for j in "${database_array[@]}"; do
		#printf "workload:  %s, database:  %s\n" "$i" "$j"
		python extract_io_summary.py tracefiles/$1/YCSB_Workload${i}_TimingA${j}.log.gz
		exit_code=$?
		#printf "return code:  %d\n" "$exit_code"
		if [ $exit_code -ne 0 ]; then
			echo "Script error"
			exit 1
		fi
		if [ "$j" != "bdb100" ]; then
			printf ", "
		fi
	done
	printf "\n"
done

