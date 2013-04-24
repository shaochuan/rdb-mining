#!/usr/bin/python
import sys
import urllib
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError
import HTMLParser
from profile import CandidatePage
from enum import enum, set_last_pid, get_finished, set_finished

def crawl(website, shard, conn):
    name = 'foo-bar'
    task = 'crawl'
    count_for_checkpoint_pid = 20

    if get_finished(conn, task, shard):
        print 'Task %s at Shard %d finished.' % (task, shard)
        return

    for loop_count, pid in enumerate(enum(conn, task, shard)):
        # remember where we are, so we can resume
        # this pid will be retried when resume
        if (loop_count+1) % count_for_checkpoint_pid == 0:
            set_last_pid(conn, task, shard, pid)

        url = 'http://www.%s.com/pub/%s/%s' % (website, name, pid)
        print url
        profile = r.table('profile').get(pid).run(conn)
        if profile:
            print '%s existed.' % (url,)
            continue
        try:
            cp = CandidatePage.from_url(url)
        except HTMLParser.HTMLParseError, e:
            cp = None
            pass
        if not cp:
            continue
        try:
            cp_dict = cp.to_dict()
            cp_dict['pid'] = pid
            r.table('profile').insert( cp.to_dict() ).run(conn)
        except RqlRuntimeError, e:
            print e
        newname = cp.first_nm + '-' + cp.last_nm
        newname = newname.replace(' ', '-').encode('utf-8')
        newname = urllib.pathname2url(newname)
        if len(newname) < 15:
            name = newname
        print url, cp.to_json()

    set_finished(conn, task, shard)
    print 'Task %s at Shard %d finished.' % (task, shard)

if __name__ == '__main__':
    website = sys.argv[1]
    server_ip_dns = sys.argv[2]
    conn = r.connect(server_ip_dns, port=28015, db='people')
    conn.repl()
    shard = int(sys.argv[3])
    crawl(website, shard, conn)
