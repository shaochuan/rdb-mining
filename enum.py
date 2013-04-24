import random
import rethinkdb as r

def numbers(n):
    if n <= 1:
        for d in g_digits:
            yield d
    else:
        for d in g_digits:
            for p in numbers(n-1):
                yield d+p

def all_digits(highest, digits, numbr1, numbr2):
    highest = list('0123456')
    random.shuffle(highest)
    digits = list(g_digits)
    random.shuffle(digits)

    numbr1 = list(numbers(3))
    random.shuffle(numbr1)
    numbr2 = list(numbers(3))
    random.shuffle(numbr2)
    return highest, digits, numbr1, numbr2

def get_permutation_order(conn):
    return r.table('metadata').get('metadata').run(conn)['permutation_order']

def enum(conn, task, shard=0, last_pid=None):
    if not last_pid:
        last_pid = get_last_pid(conn, task, shard)
    enum_started = not last_pid
    highest, digits, numbr1, numbr2 = get_permutation_order(conn)
    if isinstance(shard, (list, tuple)):
        numbr1 = map(lambda x: numbr1[x], shard)
    else:
        numbr1 = [numbr1[shard]]
    for n1 in numbr1:
        for n2 in numbr2:
            for d in digits:
                for h in highest:
                    pid = '/'.join([h+d,n1,n2])
                    if pid == last_pid:
                        enum_started = True
                    if enum_started:
                        yield pid
                    else:
                        print 'skipping %s' % (pid,)

def set_last_pid(conn, task, shard, last_pid):
    return r.table('shards').get(shard).update({task+'_last_pid': last_pid}).run(conn)

def get_last_pid(conn, task, shard):
    return r.table('shards').get(shard)[task+'_last_pid'].run(conn)

def set_finished(conn, task, shard):
    return r.table('shards').get(shard).update({task+'_finished': True}).run(conn)

def get_finished(conn, task, shard):
    return r.table('shards').get(shard)[task+'_finished'].run(conn)

