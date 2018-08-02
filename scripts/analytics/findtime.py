import sys
import json
import argparse
import gzip

parser = argparse.ArgumentParser()
parser.add_argument('dbType', type=str, help="Database type.")
parser.add_argument('input', type=str, help="Input data file.")
parser.add_argument('--summary', action="store_true", help="Don't print detailed information.")
args = parser.parse_args()

dbType = args.dbType
workload = args.input

f = open('info.info','w')
with gzip.open(workload,'r') as log:
	between = 0
	for line in log:
		if dbType + '_START' in line:
			between = 1
		if between == 0:
			continue
		if 'sched_switch' in line or 'START' in line or 'END' in line:
			f.write(line)
		if dbType + '_END' in line:
			break
f.close()

workload = 'info.info'

pids = []
my_pid = 0
block_insert = []
block_complete1 = []
data = dict()

start_time = 0.0
end_time = 0.0

# find all pids present in workload
with open(workload, 'r') as log:
	count = 0
	between = 0
	for line in log:
		if dbType + '_START' in line:
			start_time = float(line.split()[3].split(':')[0])
			pid = line.split('[0')[0].split('-')[1].split()[0]
			my_pid = pid
			name = 'benchmark'
			data[pid] = [name,0,0,0]
			pids.append(pid)
			between = 1
		if between == 0:
			continue
		if dbType + '_END' in line:
			end_time = float(line.split()[3].split(':')[0])
			between = 2
		if between == 2:
			break
		count += 1
		if 'flush-' in line:
			continue
		if 'sched_switch' in line:
			pid = line.split('[0')[0].split('-')[1].split()[0]
			if pid not in pids:
				pids.append(pid)
				name = line.split('[0')[0].split('-')[0].split()[0]
				data[pid] = [name,0.0,0.0]
count = 0

with open(workload, 'r') as log:
	between = 0
	for line in log:
		if dbType + '_START' in line:
			between = 1
			continue
		if between == 0:
			continue
		if dbType + '_END' in line:
			between = 2
		if between == 2:
			break
		if 'sched_switch' in line:
			piddd = line.split()[11].split('=')
			if 'next_pid' in piddd[0]:
				switch_in_pid = piddd[1]
				switch_in = float(line.split()[3].split(':')[0])
				with open(workload, 'r') as log2:
					sameline = 0
					for line2 in log2:
						if line == line2:
							sameline = 1
							continue
						if sameline == 0:
							continue
						if 'prev_pid='+ switch_in_pid in line2:
							switch_out = float(line2.split()[3].split(':')[0])
							runtime = (switch_out - switch_in) * 1000
							if switch_in_pid in data:
								if switch_out <= end_time:
									data[switch_in_pid][1] += 1
									data[switch_in_pid][2] += runtime
									count += 1
									break

if args.summary:
	summary = dict()
	num_of_sched_switch = 0.0
	total_wallclock_time = (end_time - start_time)*1000
	added_up_cpu_time = 0.0
	benchmarkcputime = 0.0
	benchmarkswitchcount = 0
	maxstuff = ['',0,0.0,0.0]
	for i in data:
		if 'benchmark' in data[i][0]:
			benchmarkcputime = data[i][2]
			benchmarkswitchcount = data[i][1]

		num_of_sched_switch += data[i][1]
		added_up_cpu_time += data[i][2]

		if data[i][2] > maxstuff[3]:
			maxstuff[0] = data[i][0]
			maxstuff[1] = i
			maxstuff[2] = data[i][1]
			maxstuff[3] = data[i][2]

	summary[dbType] = [['Wall Clock Time',total_wallclock_time],['Added Up CPUTime', added_up_cpu_time/2],['Total Number of Sched Switches', num_of_sched_switch],['BENCHMARK INFO',[['PID',my_pid],['CPUTime', benchmarkcputime],['Number of Sched Switches',benchmarkswitchcount]]],['Most CPU Cycles',[['Name',maxstuff[0]],['PID',maxstuff[1]],['CPUTime', maxstuff[3]],['Number of Sched Switches',maxstuff[2]]]]]
	
	print json.dumps(summary, indent=2)

else:
	print json.dumps(data, indent=2)







