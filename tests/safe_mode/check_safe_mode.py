# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

"""script for checking safeMode in DML is as expected

In test case we send `update` SQL before, in and after a specific sharding DDL
procedure, and this script checks that update binlog event added during sharding
DDL not synced phase has safeMode true, otherwise the safeMode is false.
"""

#!/usr/bin/env python
# coding: utf-8

import json
import urllib
from collections import defaultdict


class TraceType(object):
    BinlogEvent = 1
    JobEvent = 2


class OpType(object):
    UPDATE = 2
    DDL = 4


class ReplicationEvent(object):
    QUERY_EVENT = 2
    UPDATE_ROWS_EVENTv0 = 21
    UPDATE_ROWS_EVENTv1 = 24
    UPDATE_ROWS_EVENTv2 = 31

    UPDATE = [UPDATE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2]


def scan_events():
    url = 'http://127.0.0.1:8264/events/scan?limit=1000'
    f = urllib.urlopen(url)
    return json.loads(f.read())


def _trace_id_getter(d):
    return d['event']['base']['traceID'].split('.')[0]


def _binlog_pos_with_name(ev):
    current_pos = ev['event']['state']['currentPos']
    return '.'.join([current_pos['name'], str(current_pos['pos'])])


def _event_tso(ev):
    return ev['event']['base']['tso']


def _check_safe_mode(expected, safe_mode, ev, start_ev, end_ev):
    if expected != safe_mode:
        raise Exception(
            ("safe_mode should be {}, got {}\nev: {}\nstart_evn:{}\nend_ev:{}\n").format(
            expected, safe_mode, ev, start_ev, end_ev))


def main():
    update_events = defaultdict(list)
    ddl_binlogs = defaultdict(list)
    ddl_events = defaultdict(dict)
    latest_ddl = defaultdict(list)
    for events in scan_events():
        for ev in events:
            if ev['type'] == TraceType.BinlogEvent:
                if ev['event']['opType'] == OpType.UPDATE and ev['event']['eventType'] in ReplicationEvent.UPDATE:
                    update_events[_trace_id_getter(ev)].append(ev)
                if ev['event']['opType'] == OpType.DDL:
                    trace_id = _trace_id_getter(ev)
                    pos = _binlog_pos_with_name(ev)
                    ddl_binlogs[trace_id].append(pos)
                    ddl_events[trace_id][pos] = ev

    # sort and get last sharding DDL tracing events
    for trace_id, binlogs in ddl_binlogs.items():
        binlogs = sorted(set(binlogs))
        evs = ddl_events[trace_id]
        latest_ddl[trace_id].append(evs[binlogs[-2]])
        latest_ddl[trace_id].append(evs[binlogs[-1]])

    # check safe mode of each `update` binlog event is reasonable
    count = 0
    for worker, evs in latest_ddl.items():
        start = _binlog_pos_with_name(evs[0])
        end = _binlog_pos_with_name(evs[1])
        for ev in update_events[worker]:
            pos = _binlog_pos_with_name(ev)
            safe_mode = ev['event']['state'].get('safeMode', False)
            # only DML added during sharding DDL not synced has safeMode true
            if pos > end or (pos >= start and _event_tso(ev) > _event_tso(evs[1])):
                _check_safe_mode(False, safe_mode, ev, evs[0], evs[1])
            elif pos >= start:
                _check_safe_mode(True, safe_mode, ev, evs[0], evs[1])
            count += 1
    print 'check {} update events passed!'.format(count)


if __name__ == '__main__':
    main()
