# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

import sys, os
if sys.version_info[0] < 3:
    from Queue import Empty
else:
    from queue import Empty

from multiprocessing import Process, Queue

class ExceptionItem(object):
    def __init__(self, exception):
        self.exception = exception

class ParallelGeneratorException(Exception):
    pass

class GeneratorDied(ParallelGeneratorException):
    pass

class ParallelGenerator(object):
    def __init__(self,
                orig_gen,
                max_lookahead=None,
                get_timeout=10):
        """
        Creates a parallel generator from a normal one.
        The elements will be prefetched up to max_lookahead
        ahead of the consumer. If max_lookahead is None,
        everything will be fetched.

        The get_timeout parameter is the number of seconds
        after which we check that the subprocess is still
        alive, when waiting for an element to be generated.

        Any exception raised in the generator will
        be forwarded to this parallel generator.
        """
        if max_lookahead:
            self.queue = Queue(max_lookahead)
        else:
            self.queue = Queue()

        def wrapped():
            try:
                for item in orig_gen:
                    self.queue.put(item)
                raise StopIteration()
            except Exception as e:
                self.queue.put(ExceptionItem(e))

        self.get_timeout = get_timeout
        self.ppid = None # pid of the parent process
        self.process = Process(target=wrapped)
        self.process_started = False

    def finish_if_possible(self):
        """
        We can only terminate the child process from the parent process
        """
        if self.ppid == os.getpid() and self.process:# and self.process.is_alive():
            self.process.terminate()
            self.process = None
            self.queue = None
            self.ppid = None

    def __enter__(self):
        """
        Starts the process
        """
        self.ppid = os.getpid()
        self.process.start()
        self.process_started = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Kills the process
        """
        assert self.process_started and self.ppid == None or self.ppid == os.getpid()
        self.finish_if_possible()

    def __next__(self):
        return self.next()

    def __iter__(self):
        return self

    def __del__(self):
        self.finish_if_possible()

    def next(self):
        if not self.process_started:
            raise ParallelGeneratorException(
                """The generator has not been started.
                   Please use "with ParallelGenerator(..) as g:"
                """)
        try:
            item_received = False
            while not item_received:
                try:
                    item = self.queue.get(timeout=self.get_timeout)
                    item_received = True
                except Empty:
                    # check that the process is still alive
                    if not self.process.is_alive():
                        raise GeneratorDied(
                            "The generator died unexpectedly.")

            if type(item) == ExceptionItem:
                raise item.exception
            return item

        except Exception:
            self.finish_if_possible()
            raise


