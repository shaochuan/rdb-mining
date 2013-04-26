#!/usr/bin/python
import sys
import urllib
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from enum import enum, set_last_pid, get_finished, set_finished

def duration(conn, shard):
    task = 'duration'
    for loop_count, pid in enumerate(enum(conn, task, shard)):
        profile = r.table('profile').get(pid).run(conn)
        if not profile:
            print '%s not existed.' % (pid,)
            continue
        #print profile

        positions = profile.get('positions')
        if not positions:
            continue
        durations = map(lambda x:x.get('duration'), positions)
        if not any(durations):
            continue

        for days in durations:
            if days is None ort days >= 10000:
                continue
            print 'days %d incremented.' % (int(days),)
            r.table('duration').get(days).update({'n': r.row['n'] + 1}).run(conn)

if __name__ == '__main__':
    server_ip_dns = sys.argv[1]
    conn = r.connect(server_ip_dns, port=28015, db='people')
    conn.repl()
    shard = int(sys.argv[2])
    duration(conn, shard)
