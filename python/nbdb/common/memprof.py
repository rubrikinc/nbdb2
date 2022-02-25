"""
Memory profiling via dedicated thread

================================================================================
Overview
================================================================================
    Applications can use this module in order to profile process memory usage
    and detect memory leaks over time, using tracemalloc. To use the profiler
    in an app:

        import memprof
        def main(args):
            memprof.start(logger, get_config_callback, get_config_callback_arg)
            ...
            memprof.stop()

    Once the profiler is start()'ed, the memprof thread takes snapshots every
    configured interval. The application can anytime explicitly take additional
    snapshots by calling memprof.take_snapshot():

        def foo():
            ...
            if (first_call):  # let's say that snapshot upon 1st call is desired
                memprof.take_snapshot(label='foo_1st_call')
            ...

  Performance note:
    Keep in mind that tracemalloc has a memory usage overhead, mainly for
    storing stacktraces of allocated memory blocks. This is estimated to be
    3x-4x, though it would vary depending on memory allocation patterns. For
    details see:
    https://stackoverflow.com/questions/61651641/how-big-is-the-overhead-of-tracemalloc

  Supported platforms:
    This module was verified on both Mac and Linux (dev VM).

================================================================================
Module API
================================================================================
    memprof.start(logger, get_config_callback, get_config_callback_arg)
    memprof.stop()
    memprof.take_snapshot(label)

  [helpers for start()'s get_config_callback and get_config_callback_arg params]

    memprof.validate_config(config)
    memprof.parse_config_from_json_file(config_file)

================================================================================
Configuration (used by memprof thread)
================================================================================

    memprof config is a dict with the following schema, as seen in
    Thread.s_default_config (the source of truth).

    s_default_config = {
        'enable': False,             # enable snapshots in memprof thread
        'poll_for_enable_sec': 30,   # while disabled, poll till enabled
        'snapshot_interval_sec': 10, # how often to take snapshots
        'dump_to_disk_enable': True, # dump snapshots to disk
        'dump_to_disk_prefix': '/tmp/snapshot', # snapshot path prefix for dump,
                                     # note: parent folder must already exist
        'top_k_stats': 10,           # only topK leaks are logged (specify K)
        'trace_filters_include': [], # keep only traces that match (only
                                     # applies to log, disk dump is unfiltered)
        'trace_filters_exclude': ['<unknown>'], # drop traces that match (only
                                     # applies to log, disk dump is unfiltered)
    }

================================================================================
Logging
================================================================================
    memprof logs into a logger provided by the application. All messages
    logged by the memprof module have a MEMPROF prefix for easy filtering.

================================================================================
Snapshot print/diff utility (see main)
================================================================================
    The module's main provides options for printing and diffing snapshots that
    were saved to disk (dump to disk is enabled by default in memprof config).
    Examples:

    [Print snapshot, excluding traces for python framework files (Cellar on
     mac using brew-installed python)]

        $ python3 python/nbdb/common/memprof.py --print /tmp/memprof_snapshot_20211125_00_27_44_end-of-main -k 3 -E "*Cellar*"
        MEMPROF print /tmp/memprof_snapshot_20211125_00_27_44_end-of-main:
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:415: size=6000 MiB, count=5963, average=1030 KiB
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:417: size=25.4 KiB, count=1, average=25.4 KiB
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:179: size=1448 B, count=2, average=724 B

    [Diff snapshots]

        $ python3 python/nbdb/common/memprof.py --diff /tmp/memprof_snapshot_20211122_02_36_26_snap-1 /tmp/memprof_snapshot_20211122_02_37_33_snap-7 -k 3

        MEMPROF diff /tmp/memprof_snapshot_20211122_02_36_26_snap-1 => /tmp/memprof_snapshot_20211122_02_37_33_snap-7:
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:338: size=6000 MiB (+5998 MiB), count=5965 (+5964), average=1030 KiB
        MEMPROF  /usr/local/Cellar/python@3.9/3.9.8/Frameworks/Python.framework/Versions/3.9/lib/python3.9/tracemalloc.py:469: size=46.5 KiB (+45.3 KiB), count=3 (+1), average=15.5 KiB
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:340: size=25.4 KiB (+25.3 KiB), count=1 (+0), average=25.4 KiB

      Adding an exclude trace filter to drop tracemalloc.py memory allocations:

        $ python3 python/nbdb/common/memprof.py --diff /tmp/memprof_snapshot_20211122_02_36_26_snap-1 /tmp/memprof_snapshot_20211122_02_37_33_snap-7 -k 3 -E "*tracemalloc.py"

        MEMPROF diff /tmp/memprof_snapshot_20211122_02_36_26_snap-1 => /tmp/memprof_snapshot_20211122_02_37_33_snap-7:
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:338: size=6000 MiB (+5998 MiB), count=5965 (+5964), average=1030 KiB
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:340: size=25.4 KiB (+25.3 KiB), count=1 (+0), average=25.4 KiB
        MEMPROF  /usr/local/Cellar/python@3.9/3.9.8/Frameworks/Python.framework/Versions/3.9/lib/python3.9/threading.py:956: size=658 B (+658 B), count=2 (+2), average=329 B

      Adding an include trace filter to only show nbdb src memory allocations:

        $ python3 python/nbdb/common/memprof.py --diff /tmp/memprof_snapshot_20211122_02_36_26_snap-1 /tmp/memprof_snapshot_20211122_02_37_33_snap-7 -k 3 -I "*/nbdb/*"

        MEMPROF diff /tmp/memprof_snapshot_20211122_02_36_26_snap-1 => /tmp/memprof_snapshot_20211122_02_37_33_snap-7:
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:338: size=6000 MiB (+5998 MiB), count=5965 (+5964), average=1030 KiB
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:340: size=25.4 KiB (+25.3 KiB), count=1 (+0), average=25.4 KiB
        MEMPROF  /Users/ihen/work/sdmain_master/nbdb/python/nbdb/common/memprof.py:196: size=472 B (+472 B), count=1 (+1), average=472 B

      NOTE: multiple trace filters (-I and/or -E) can be provided.

================================================================================
Testing utility (see main)
================================================================================
    The module' main provides options for showcasing the memory profiling
    capabilities in a multi-threaded application. It starts memprof and spawns
    two sets of worker threads that would all get profiled:
        1. a set of leaking worker threads (default: 100)
        2. a set of non-leaking worker threads (default: 100)
    See memprof.log to examine the profiling results, showing the top-k
    leaks (based on the diff between periodic snapshots).

    Running the main with no args (i.e. default args) takes ~90 seconds and
    leaks a total of ~6 GB of RAM. Run with --help for options, see below common
    usage examples:

    <<< Example 1: run only with non-leaking workers >>>

    $ python3 python/nbdb/common/memprof.py --leak_workers=0 --no_leak_workers=100
    $ tail -7 memprof.log
    2021-11-21_17:14:21|INFO    |MEMPROF snapshot "memprof-7" taken
    2021-11-21_17:14:21|INFO    |MEMPROF diff 2021-11-21 17:14:11.193823 => 2021-11-21 17:14:21.198987:
    MEMPROF  tracemalloc.py:193: size=6288 B (+624 B), count=131 (+13), average=48 B
    MEMPROF  tracemalloc.py:135: size=448 B (+448 B), count=1 (+1), average=448 B
    MEMPROF  tracemalloc.py:125: size=0 B (-448 B), count=0 (-1)
    2021-11-21_17:14:22|INFO    |Stop memprof ...
    2021-11-21_17:14:31|INFO    |MEMPROF profiler thread exiting after 0:01:20.051709, total snapshots taken: 8

    <<< Example 2: run only with leaking workers >>>

    $ python3 python/nbdb/common/memprof.py --leak_workers=100 --no_leak_workers=0
    $ tail -7 memprof.log
    2021-11-21_17:16:34|INFO    |MEMPROF snapshot "memprof-6" taken
    2021-11-21_17:16:34|INFO    |MEMPROF diff 2021-11-21 17:16:21.351377 => 2021-11-21 17:16:32.007945:
    MEMPROF  memprof.py:271: size=6000 MiB (+1000 MiB), count=5969 (+999), average=1029 KiB
    MEMPROF  memprof.py:273: size=25.4 KiB (+5504 B), count=1 (+0), average=25.4 KiB
    MEMPROF  tracemalloc.py:558: size=6552 B (+496 B), count=135 (+10), average=49 B
    2021-11-21_17:16:34|INFO    |Stop memprof ...
    2021-11-21_17:16:44|INFO    |MEMPROF profiler thread exiting after 0:01:17.466681, total snapshots taken: 7

"""
import argparse
import datetime
import json
import logging
import os
import sys
import threading
import time
import tracemalloc


# ==============================================================================
# class _Thread (note: this is not part of the module's API)
# ==============================================================================

class _Thread(threading.Thread):

    def __init__(self, logger, get_config_callback, get_config_callback_arg,
                 ready_event):
        """Ctor"""
        threading.Thread.__init__(self)

        # memory profiler configuration - provided by app
        self.m_logger = logger
        self.m_get_config_callback = get_config_callback
        self.m_get_config_callback_arg = get_config_callback_arg
        self.m_config = _Thread.s_default_config

        # internal state (snapshot info etc.)
        self.m_ready_event = ready_event
        self.m_config_write_lock = threading.Lock()
        self.m_take_snapshot_lock = threading.Lock()
        self.m_prev_snapshot = None
        self.m_prev_snapshot_ts = 0
        self.m_marked_for_stop = False

    @staticmethod
    def validate_config(config):
        """Returns True only if config is valid"""
        keys_set = set(_Thread.s_default_config.keys())
        inter_set = keys_set.intersection(config.keys())
        return inter_set == keys_set

    def __refresh_config_from_app(self):
        """Fetch and apply the config from app using registered callback"""
        config = self.m_get_config_callback(self.m_get_config_callback_arg)
        if config == self.m_config:
            return

        # validate the new config
        if not validate_config(config):
            self.m_logger.error(f'MEMPROF memory profiler config is invalid! '
                                f'not applying new config: {config}')
            return

        # apply the new config.
        # note: other than the memprof thread, application threads may access
        # the config only via the take_snapshot() API call. take_snapshot()
        # clones the config and uses the copy in processing, to avoid holding
        # the config lock during the whole snapshot processing (which is
        # much longer than the cloning).
        with self.m_config_write_lock:
            self.m_logger.info(f'MEMPROF applying new config: {config} (prev '
                               f'config: {self.m_config})')
            self.m_config = config

    def __take_snapshot_no_lock(self, config, label=''):
        """Take a snapshot, dump it to disk, and log diff from prev snapshot"""
        curr_snapshot_ts = datetime.datetime.now()
        curr_snapshot = tracemalloc.take_snapshot()

        # create a copy of the snapshot and apply trace filters from config.
        # note: the filtered snapshot is only used for logging of the diff;
        # dump to disk uses the full snapshot, to allow filtering later.
        trace_filters = []
        for pattern in config['trace_filters_include']:
            trace_filters.append(tracemalloc.Filter(True, pattern))
        for pattern in config['trace_filters_exclude']:
            trace_filters.append(tracemalloc.Filter(False, pattern))
        curr_snapshot_filtered = curr_snapshot.filter_traces(trace_filters)

        # if no label provided, use auto-generated label with running index
        lbl = label if label else f'snap-{_Thread.s_snap_cnt}'

        # dump unfiltered snapshot to disk (if enabled)
        snapshot_file = 'None'
        if config['dump_to_disk_enable']:
            snapshot_file = config['dump_to_disk_prefix'] + '_' + \
                            curr_snapshot_ts.strftime('%Y%m%d_%H_%M_%S') + \
                            '_' + lbl
            curr_snapshot.dump(snapshot_file)  # use unfiltered snapshot

        self.m_logger.info(f'MEMPROF snapshot taken, label="{lbl}", disk-file='
                           f'{snapshot_file}')

        if self.m_prev_snapshot is not None:
            top_stats = curr_snapshot_filtered.compare_to(self.m_prev_snapshot,
                                                          'lineno')
            s = f'MEMPROF diff {self.m_prev_snapshot_ts} => {curr_snapshot_ts}:'
            for stat in top_stats[:config['top_k_stats']]:
                s += f'\nMEMPROF  {str(stat)}'
            self.m_logger.info(s)

        _Thread.s_snap_cnt += 1
        self.m_prev_snapshot_ts = curr_snapshot_ts
        self.m_prev_snapshot = curr_snapshot_filtered  # use filtered snapshot

    # note: take_snapshot() is thread-safe - once start()'ed, take_snapshot()
    # can be called from any app context
    def take_snapshot(self, label=''):
        """Take snapshot synchronized wrapper"""
        with self.m_take_snapshot_lock:
            # clone the config and use the copy in processing, to avoid holding
            # the config lock during the whole snapshot processing (which is
            # much longer than the cloning).
            with self.m_config_write_lock:
                config_copy = self.m_config
            self.__take_snapshot_no_lock(config_copy, label)

    def mark_for_stop(self):
        """Mark the thread for stop - it will exit upon wakeup"""
        self.m_marked_for_stop = True

    # thread main - poll for stop or config enablement.
    # when enabled, take snapshots in configured interval.
    def run(self):
        """Thread main - loop and take snapshots if enabled in config"""
        start_time = datetime.datetime.now()
        self.__refresh_config_from_app()
        self.m_ready_event.set()  # signal the event indicating memprof is ready

        while not self.m_marked_for_stop:
            if self.m_config['enable']:
                self.take_snapshot()
                time.sleep(self.m_config['snapshot_interval_sec'])
            else:
                time.sleep(self.m_config['poll_for_enable_sec'])
            self.__refresh_config_from_app()

        duration = datetime.datetime.now() - start_time
        self.m_logger.info(f'MEMPROF profiler thread exiting after {duration}, '
                           f'total snapshots taken: {_Thread.s_snap_cnt}')
        _Thread.s_snap_cnt = 0

    s_snap_cnt = 0  # incremented every take_snapshot()
    s_default_config = {
        'enable': False,
        'poll_for_enable_sec': 30,
        'snapshot_interval_sec': 900,
        'dump_to_disk_enable': True,
        'dump_to_disk_prefix': '/tmp/memprof_snapshot',
        'top_k_stats': 10,
        'trace_filters_include': [],
        'trace_filters_exclude': ['<unknown>']
    }
    s_invalid_config = {
        'invalid': 'check for errors in the memprof config!'
    }


__g_thread: _Thread = None
__g_thread_lock = threading.Lock()


# ==============================================================================
# MODULE API
# ==============================================================================

def validate_config(config):
    """
    Validate the given memprof config (dict)
    """
    return _Thread.validate_config(config)


def parse_config_from_json_file(config_file):
    """
    Parses a JSON from given file.
    This function can be used as a parameter for start()'s get_config_callback;
    the get_config_callback_arg would be the config file to parse the json from.
    """
    if not os.path.exists(config_file):
        return _Thread.s_default_config
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.loads(f.read())
    except (OSError, json.JSONDecodeError):
        # since the config may end up having typo's due to manual edits, return
        # an invalid config here and keep using the current valid config.
        return _Thread.s_invalid_config


def start(logger, get_config_callback, get_config_callback_arg):
    """
    Start memprof - if tracemalloc is not tracing, start tracing. Create the
    memprof thread to take snapshots and wait for it to signal it's ready.
    """
    global __g_thread
    with __g_thread_lock:
        if __g_thread is not None:
            return False
        started_tracing = False
        if not tracemalloc.is_tracing():
            started_tracing = True
            tracemalloc.start()
        ready_event = threading.Event()
        __g_thread = _Thread(logger, get_config_callback,
                             get_config_callback_arg, ready_event)
        __g_thread.start()
        if not ready_event.wait(60):
            # should not happen, unless CPU is extremely saturated: memprof
            # thread is still not UP, after 1 minute.
            # - assume that memprof will not work from now on
            # - stop tracemalloc (only if we started it)
            # - do not attempt to join the thread here, as it would block -
            #   it's better to just leak the thread at this point (very minor).
            # - just in case the memprof thread does start at some point - make
            #   sure it would immediately abort, by marking it for stop here.
            __g_thread.mark_for_stop()
            if started_tracing:
                tracemalloc.stop()
            return False
        ready_event.clear()  # reset the event for subsequent start() calls
        return True


def stop():
    """
    Stop memprof - if tracemalloc is tracing, stop tracing. Signal the memprof
    thread to stop and join it. Set global __g_thread to None to allow
    calling start() again by app.
    """
    global __g_thread
    with __g_thread_lock:
        if __g_thread is None:
            return False
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        __g_thread.mark_for_stop()
        __g_thread.join()
        __g_thread = None
        return True


def take_snapshot(label):
    """
    Take snapshot - a snapshot would be taken in addition to the snapshots
    that the memprof thread is taking. This is thread-safe.
    """
    with __g_thread_lock:
        if __g_thread is None:
            return False
        __g_thread.take_snapshot(label)
        return True


# ==============================================================================
# MAIN - used as either a:
#   1. snapshot utility to print/diff snapshot files
#   2. test utility that uses memprof in a multi-threaded app
# ==============================================================================

class _WorkerThread(threading.Thread):
    def __init__(self, leak_size=0):
        threading.Thread.__init__(self)
        self.m_leak_size = leak_size

    def run(self):
        if self.m_leak_size > 0:
            # leak allocated memory by inserting to the global leak_list
            mem = [1] * int(self.m_leak_size / 4)
            with _WorkerThread.s_leak_list_lock:
                _WorkerThread.s_leak_list.append(mem)
            return None

        # else - alloc some memory that would get released (i.e. no leak)
        mem = [1] * (1024 * 1024)
        return mem

    s_leak_list = []
    s_leak_list_lock = threading.Lock()


def __parse_args():
    parser = argparse.ArgumentParser(
        description='This utility can be run as a snapshot utility ('
                    '-p|--print_snapshot, -d|--diff_snapshots) or as a test '
                    'utility that spawns worker threads ('
                    '-f|--memprof_config_file and options beneath it)')

    # When used as a snapshot utility:
    parser.add_argument('-p', '--print_snapshot',
                        help='print snapshot file top-k stats',
                        type=str, action='store', dest='print_snapshot',
                        default='')
    parser.add_argument('-d', '--diff_snapshots',
                        help='diff two snapshot files',
                        type=str, action='store', nargs=2,
                        dest='diff_snapshots',
                        default=['', ''])
    parser.add_argument('-k', '--top_k',
                        help='show only top-k stats in print or diff',
                        type=int, action='store', dest='top_k',
                        default=10)
    parser.add_argument('-I', '--trace_filters_include',
                        help='include pattern for matching traces',
                        type=str, action='append', dest='trace_filters_include',
                        default=[])
    parser.add_argument('-E', '--trace_filters_exclude',
                        help='exclude pattern for matching traces',
                        type=str, action='append', dest='trace_filters_exclude',
                        default=[])

    # When run as a test utility:
    parser.add_argument('-f', '--memprof_config_file',
                        help='memprof json config file',
                        type=str, action='store', dest='memprof_config_file',
                        default='/tmp/memprof_config.json')
    parser.add_argument('-n1', '--leak_workers',
                        help='num of memory-leaking workers',
                        type=int, action='store', dest='n_leak_workers',
                        default=100)
    parser.add_argument('-n2', '--no_leak_workers',
                        help='num of non-memory-leaking workers',
                        type=int, action='store', dest='n_no_leak_workers',
                        default=100)
    parser.add_argument('-i', '--interval',
                        help='workers creation interval (in seconds)',
                        type=int, action='store', dest='interval',
                        default=2)
    parser.add_argument('-N', '--iterations',
                        help='number of iterations of workers creation',
                        type=int, action='store', dest='iterations',
                        default=30)
    parser.add_argument('-s', '--leak_size',
                        help='memory leak size for a single worker (in bytes)',
                        type=int, action='store', dest='leak_size',
                        default=1024 * 1024)
    return parser.parse_args()


def __snapshot_utility_main(args):

    # -p, --print_snapshot
    if args.print_snapshot:
        if not os.path.exists(args.print_snapshot):
            print(f'ERROR: invalid snapshot file {args.print_snapshot}',
                  file=sys.stderr)
            return 1

        snapshot = tracemalloc.Snapshot.load(args.print_snapshot)

        trace_filters = []
        for pattern in args.trace_filters_include:
            trace_filters.append(tracemalloc.Filter(True, pattern))
        for pattern in args.trace_filters_exclude:
            trace_filters.append(tracemalloc.Filter(False, pattern))
        snapshot = snapshot.filter_traces(trace_filters)

        top_stats = snapshot.statistics('lineno')
        s = f'MEMPROF print {args.print_snapshot}:'
        for stat in top_stats[:args.top_k]:
            s += f'\nMEMPROF  {str(stat)}'
        print(s)
        return 0

    # -d, --diff_snapshots
    if args.diff_snapshots[0]:
        if not os.path.exists(args.diff_snapshots[0]):
            print(f'ERROR: invalid snapshot file {args.diff_snapshots[0]}',
                  file=sys.stderr)
            return 1
        if not os.path.exists(args.diff_snapshots[1]):
            print(f'ERROR: invalid snapshot file {args.diff_snapshots[1]}',
                  file=sys.stderr)
            return 1
        try:
            snapshot1 = tracemalloc.Snapshot.load(args.diff_snapshots[0])
            snapshot2 = tracemalloc.Snapshot.load(args.diff_snapshots[1])

            # apply trace filters to both snapshots
            trace_filters = []
            for pattern in args.trace_filters_include:
                trace_filters.append(tracemalloc.Filter(True, pattern))
            for pattern in args.trace_filters_exclude:
                trace_filters.append(tracemalloc.Filter(False, pattern))
            snapshot1 = snapshot1.filter_traces(trace_filters)
            snapshot2 = snapshot2.filter_traces(trace_filters)

            top_stats = snapshot2.compare_to(snapshot1, 'lineno')
            s = f'MEMPROF diff {args.diff_snapshots[0]} => ' \
                f'{args.diff_snapshots[1]}:'
            for stat in top_stats[:args.top_k]:
                s += f'\nMEMPROF  {str(stat)}'
            print(s)
            return 0

        except Exception as e:
            print(f'ERROR: caught exception: {str(e)}', file=sys.stderr)
            return 1

    assert False, "missing return statement in above case handling!"


def __test_utility_main(args):
    logging.basicConfig(filename='memprof.log', filemode='w',
                        level=logging.INFO, datefmt='%Y-%m-%d_%H:%M:%S',
                        format='%(asctime)s|%(levelname)-8s|%(message)s')
    logger = logging.getLogger(__name__)

    try:
        logger.info('Start memprof ...')
        if not start(logger, parse_config_from_json_file,
                     args.memprof_config_file):
            logger.error('Failed to start memprof!')
        logger.info('memprof has started')

        take_snapshot('start-of-main')

        for i in range(args.iterations):
            logger.info(f'Iter {i + 1}/{args.iterations}: start workers, '
                        f'leak_workers={args.n_leak_workers} no_leak_workers='
                        f'{args.n_no_leak_workers}')
            for _ in range(args.n_leak_workers):
                _WorkerThread(args.leak_size).start()
            for _ in range(args.n_no_leak_workers):
                _WorkerThread().start()
            time.sleep(args.interval)

        take_snapshot('end-of-main')
        return 0
    finally:
        logger.info('Stop memprof ...')
        if not stop():
            logger.error('Failed to stop memprof!')
        logger.info('memprof is stopped')


def __main(args):
    if args.print_snapshot or args.diff_snapshots[0]:
        # run as a snapshot print/diff utility
        return __snapshot_utility_main(args)

    # run as a test utility
    return __test_utility_main(args)


if __name__ == '__main__':
    __main(__parse_args())
