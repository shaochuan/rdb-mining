#!/usr/bin/python
import sys
import urllib
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from enum import enum, set_last_pid, get_finished, set_finished

def make_index(conn, shard, idx='company'):
    task = 'index'
    count_for_checkpoint_pid = 20

    if get_finished(conn, task, shard):
        print 'Task %s at Shard %d finished.' % (task, shard)
        return

    for loop_count, pid in enumerate(enum(conn, task, shard)):
        # remember where we are, so we can resume
        # this pid will be retried when resume
        print 'indexing %s...' % (pid,)
        if (loop_count+1) % count_for_checkpoint_pid == 0:
            set_last_pid(conn, task, shard, pid)

        profile_idx = r.table('profile_index').get(pid).run(conn)
        if not profile_idx:
            r.table('profile_index').insert({'pid':pid, 'indices':[]}).run(conn)
            profile_idx = r.table('profile_index').get(pid).run(conn)

        indices = profile_idx['indices']
        if idx in indices:
            print '%s indexed.' % (pid,)
            # this means this pid has been indexed before
            continue

        profile = r.table('profile').get(pid).run(conn)
        if not profile:
            print '%s not existed.' % (pid,)
            continue
        print profile

        positions = profile.get('positions')
        if not positions:
            continue
        entry_or_list = map(lambda x:x.get('org'), positions)
        if not any(entry_or_list):
            continue
        if not isinstance(entry_or_list, list):
            entry_or_list = [entry_or_list]
        primary_key = idx[0]+'id'
        print entry_or_list
        for entry in entry_or_list:
            if not entry:
                continue
            pids = r.table(idx).get(entry).run(conn)
            if not pids:
                pids = []
                r.table(idx).insert({primary_key: entry, 'pids':pids}).run(conn)
            else:
                pids = pids.get('pids', [])
            print pids
            if pid in pids:
                # this means this pid's index has been processed before
                continue
            print (entry, pid)
            r.table(idx).get(entry).update({'pids': r.row['pids'].append(pid)}).run(conn)
        # tell indexer that we're done
        r.table('profile_index').get(pid).update({'indices': r.row['indices'].append(idx)}).run(conn)

    set_finished(conn, task, shard)
    print 'Task %s at Shard %d finished.' % (task, shard)

if __name__ == '__main__':
    server_ip_dns = sys.argv[1]
    conn = r.connect(server_ip_dns, port=28015, db='people')
    conn.repl()
    shard = int(sys.argv[2])
    make_index(conn, shard)
