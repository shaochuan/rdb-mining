#!/usr/bin/python
import sys
import urllib
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
from profile import CandidatePage
from enum import enum

def make_index(shard, idx='skills', db, conn):
    for pid in enum(shard, db, conn):
        profile = db.table('profile_index').get(pid).run(conn)
        if not profile:
            db.table('profile_index').insert({'pid':pid, 'indices':[]}).run(conn)
            profile = db.table('profile_index').get(pid).run(conn)
        indices = profile['indices']
        if idx in indices:
            # this means this pid has been indexed before
            continue

        indices.append(idx)

	try:
            entry_or_list = db.table('profile').get(pid)[idx].run(conn)
        except RqlRuntimeError, e:
            continue
        if not entry_or_list:
            continue
        if not isinstance(entry_or_list, list):
            entry_or_list = [entry_or_list]
        primary_key = idx[0]+'id'
        for entry in entry_or_list:
            db.table(idx).insert({primary_key: entry, 'pids':[]}).run(conn)
            pids = db.table(idx).get(entry)['pids'].run(conn)
            if pid in pids:
                # this means this pid's index has been processed before
                continue
            db.table(idx).get(entry).update({'pids': r.row['pids'].append(pid)}).run(conn)
        # tell indexer that we're done
        db.table('profile_index').update(profile).run(conn)

if __name__ == '__main__':
    server_ip_dns = sys.argv[1]
    conn = r.connect(server_ip_dns, port=28015, db='profile')
    conn.repl()
    db = r.db('people')
    shard = int(sys.argv[2])
    make_index(shard)
