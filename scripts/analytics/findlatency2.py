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
log = []
block_insert = []
block_complete1 = []

start_time = 0.0
end_time = 0.0

with gzip.open(workload, 'r') as log:
	for line in log:
		if dbType + '_START' in line:
			start_time = float(line.split()[3].split(':')[0])
		if dbType + '_END' in line:
			end_time = float(line.split()[3].split(':')[0])

insert_complete = dict()
count = 0
with gzip.open(workload, 'r') as log:
	found = 0
	for line in log:
		if dbType + '_START' in line:
			found = 1
		if dbType + '_END' in line:
			found = 2
		if found == 0:
			continue
		if found == 2:
			break
		if 'block_rq_insert' in line:
			timestamp = float(line.split()[3].split(':')[0])
			insertaddr = line.split()[9] + ' + ' + line.split()[11]
			if 'withjson' not in line:
				continue
			
			with gzip.open(workload, 'r') as log2:
				sameline = 0
				for line2 in log2:
					if line == line2:
						sameline = 1
					if sameline == 0:
						continue
					if 'block_rq_complete' in line2:
						if insertaddr in line2:
							insert_complete[count] = [line,line2]
							count += 1
							break

data = dict()
for d in insert_complete:
	insert_string = insert_complete[d][0]
	insertaddr = insert_string.split()[9] + ' + ' + insert_string.split()[11]
	inserttimestamp = float(insert_string.split()[3].split(':')[0])
	insertop = insert_string.split()[6]

	complete_string = insert_complete[d][1]
	completeaddr = complete_string.split()[8] + ' + ' + complete_string.split()[10]
	completetimestamp = float(complete_string.split()[3].split(':')[0])
	completeop = complete_string.split()[6]
	assert insertaddr == completeaddr
	assert completetimestamp > inserttimestamp
	start = (inserttimestamp - start_time) * 1000
	end = (completetimestamp - start_time) * 1000
	assert start > 0
	assert end > 0
	assert (end - start) > 0
	data[d] = [start, end, end - start, insertop, completeop, insertaddr]

if args.summary:
	summary = dict()
	r_min = 10000.0
	r_max = 0.0
	r_avg = 0.0
	r_count = 0

	w_min = 10000.0
	w_max = 0.0
	w_avg = 0.0
	w_count = 0

	f_min = 10000.0
	f_max = 0.0
	f_avg = 0.0
	f_count = 0

	for i in data:
		op = data[i][3]
		runtime = data[i][2]
		if 'R' in op:
			r_count += 1
			if r_min > runtime:
				r_min = runtime
			if r_max < runtime:
				r_max = runtime
			r_avg += runtime
			#print runtime
		if 'W' in op:
			w_count += 1
			if w_min > runtime:
				w_min = runtime
			if w_max < runtime:
				w_max = runtime
			w_avg += runtime
		if 'F' in op:
			f_count += 1
			if f_min > runtime:
				f_min = runtime
			if f_max < runtime:
				f_max = runtime
			f_avg += runtime

	if r_count == 0:
		r_average = r_avg/(r_count+1)
	else:
		r_average = r_avg/r_count
	if w_count == 0:
		w_average = w_avg/(w_count+1)
	else:
		w_average = w_avg/w_count
	if f_count == 0:
		f_average = f_avg/(f_count+1)
	else:
		f_average = f_avg/f_count

	if r_min == 10000.0:
		r_min = 0.0
	if w_min == 10000.0:
		w_min = 0.0
	if f_min == 10000.0:
		f_min = 0.0

	r_count25 = 0
	w_count25 = 0
	f_count25 = 0

	for i in data:
		runtime = data[i][2]
		op = data[i][3]
		if 'R' in op:
			rupper25 = (r_average/100)*150
			rlower25 = (r_average/100)*50
			if runtime <= rlower25:
				r_count25 += 1
			if runtime >= rupper25:
				r_count25 += 1
		if 'W' in op:
			wupper25 = (w_average/100)*150
			wlower25 = (w_average/100)*50
			if runtime <= wlower25:
				w_count25 += 1
			if runtime >= wupper25:
				w_count25 += 1
		if 'F' in op:
			fupper25 = (f_average/100)*150
			flower25 = (f_average/100)*50
			if runtime <= flower25:
				f_count25 += 1
			if runtime >= fupper25:
				f_count25 += 1


	summary['READS'] = [r_count ,r_min, r_max, r_average, r_count25]
	summary['WRITES'] = [w_count, w_min, w_max, w_average, w_count25]
	summary['FLUSHES'] = [f_count, f_min, f_max, f_average, f_count25]

	print json.dumps(summary, indent=2)
else:
	print json.dumps(data, indent=2)	





