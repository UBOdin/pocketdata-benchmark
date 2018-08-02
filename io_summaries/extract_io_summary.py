#!/usr/bin/env python

import argparse, gzip, re, json

import sys

parser = argparse.ArgumentParser()
parser.add_argument('input', type=str, help="Input data file.")
parser.add_argument('--summary', action="store_true", help="Don't print detailed information.")
args = parser.parse_args()

linePrefix = re.compile(r"""^\s*(?P<name>.*?)-(?P<PID>\d+)\s+\[(?P<CPU>\d{3})\]\s+(?:\S{4})\s+(?P<timestamp>[\d\.]+):\s+(?P<message>.*?)$""")

startEndMessage = re.compile(r"""^tracing_mark_write: \{"EVENT":"(?P<DB>.*?)_(?P<operation>START|END)"\}$""");
switchMessage = re.compile(r"""^sched_switch:.*?prev_pid=(?P<outPID>\d+).*?next_pid=(?P<inPID>\d+)""")
blockMessage = re.compile(r"""^block_rq_(?P<operation>insert|complete): (?:\d+),(?:\d+) (?P<RWS>\w+) .*?(?P<address>\d+) \+ (?P<offset>\d+)""")

currentDB = None

outputHash = {}
runningHash = {}
blockHash = {}
      
blockStartTypes = ['R', 'W', 'WS', 'RM', 'FWS', 'FWFS']
blockEndTypes = ['RA', 'WA', 'WAS', 'RAM', 'WS', 'WFS']

for i in range(0, 4):
	runningHash[i] = {
		'startTime': 0,
		'PID': -1
	}
#end_for 

with gzip.open(args.input) as inputfile:
  for line in inputfile:
    line = line.strip()
    if line == "":
      continue
    prefix = linePrefix.match(line)
    if not prefix:
      continue

    PID, CPU, timestamp = int(prefix.group('PID')), int(prefix.group('CPU')), float(prefix.group('timestamp'))
    message = prefix.group('message').strip()
    if currentDB:
      output = outputHash[currentDB]
    try:
      running = runningHash[CPU]
    except:
      running = None
    
    switch = switchMessage.match(message)
    if switch:
      if currentDB and running['PID'] == output['PID']:
        runTime = timestamp - running['startTime']
        assert runTime > 0
        output['CPUExtents'].append({
          'CPU': CPU,
          'runTime': runTime,
          'startCPUTime': output['CPUTime'],
          'endCPUTime': output['CPUTime'] + runTime,
          'startTime': running['startTime'],
          'endTime': timestamp
        })
        output['CPUTime'] += runTime
        output['CPUExtentCount'] += 1
        try:
          output['CPUByCore'][CPU]['count'] += 1
          output['CPUByCore'][CPU]['runTime'] += runTime
        except:
          output['CPUByCore'][CPU] = {
            'count': 1,
            'runTime': runTime
          }
      runningHash[CPU] = {
        'startTime': timestamp,
        'PID': int(switch.group('inPID'))
      }
      continue

    startEnd = startEndMessage.match(message)
    if startEnd:
      if not currentDB:
        assert startEnd.group('operation') == 'START'
        assert running['PID'] == PID
        currentDB = startEnd.group('DB')
        assert currentDB not in outputHash
        outputHash[currentDB] = {
          'startTime': running['startTime'],
          'PID': PID,
          'CPUTime': 0,
          'CPUExtents': [],
          'CPUExtentCount': 0,
          'CPUByCore': {},
          'blockIOs': [],
          'blockIOCount': 0,
          'blockByType': {}
        }
        continue
      else:
        assert startEnd.group('operation') == 'END'
        assert startEnd.group('DB') == currentDB
        output['endTime'] = timestamp
        output['wallTime'] = output['endTime'] - output['startTime']
        assert output['wallTime'] > 0
        assert output['CPUExtentCount'] > 0
        assert output['blockIOCount'] > 0

        currentDB = None
        continue
    
    block = blockMessage.match(message)
    if block:
      assert block.group('RWS') in blockStartTypes + blockEndTypes, line
      blockAddress = int(block.group('address')) + int(block.group('offset'))
      if block.group('operation') == 'insert' and \
         currentDB and PID == output['PID']:
        assert block.group('RWS') in blockStartTypes
        assert blockAddress not in blockHash
        blockHash[blockAddress] = {
          'startTime': timestamp,
          'operation': block.group('RWS')
        }
      elif block.group('operation') == 'complete' and blockAddress in blockHash:
        blockInfo = blockHash[blockAddress]
        assert block.group('RWS') in blockEndTypes
        assert blockEndTypes.index(block.group('RWS')) == blockStartTypes.index(blockInfo['operation']), line
        blockInfo['endTime'] = timestamp
        blockInfo['waitTime'] = timestamp - blockInfo['startTime']
        output['blockIOs'].append(blockInfo)
        output['blockIOCount'] += 1
        try:
          output['blockByType'][blockInfo['operation']]['count'] += 1
          output['blockByType'][blockInfo['operation']]['waitTime'] += blockInfo['waitTime']
        except:
          output['blockByType'][blockInfo['operation']] = {
            'count': 1,
            'waitTime': blockInfo['waitTime']
          }
        del(blockHash[blockAddress])

#print output["CPUTime"]
#print output["wallTime"]

foo = str(output["CPUTime"]) + ", " + str(output["wallTime"])
sys.stdout.write(foo)



sys.exit(0)

if args.summary:
  for data in outputHash.values():
    del(data['CPUExtents'])
    del(data['blockIOs'])

print json.dumps(outputHash, indent=2)

# vim: ts=2:sw=2:et
