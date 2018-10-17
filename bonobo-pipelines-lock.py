# Execute: bonobo run bonobo-lock.py

import bonobo
import logging
import threading
from bonobo.config import use
import time

@use('lock')
def acquire_lock(lock):
    logging.info('acquire_lock: START')
    status = lock.acquire()
    return status

@use('lock')
def release_lock(input, lock):
    logging.info('release_lock: START')
    lock.release()

@use('lock')
def a(status, lock):
    logging.info('a: START')
    return 42

@use('lock')
def b(id, lock):
    # This Node's operation that MUST NOT proceed until Node 'd' has completed successfully
    logging.info('b: START')
    while lock.locked():
        logging.info('b: unable to acquire lock')
        time.sleep(2)
    logging.info('b: lock acquired %d' % id)

def c(id):
    logging.info('c: START')
    logging.info('c: start long operation')
    time.sleep(10) # fake that this node does work that takes some time
    return 'Hello'

def d(name):
    logging.info('d: START')
    logging.info('d: %s' % name)
    return name

def get_graph(**options):

    graph = bonobo.Graph()

    graph.add_chain(
        acquire_lock, 
        a, 
        b)
    
    graph.add_chain(
        c,
        d,
        release_lock,
        _input = a)

    return graph

def get_services(**options):

    return {
        'lock' : threading.Lock()
    }

# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )