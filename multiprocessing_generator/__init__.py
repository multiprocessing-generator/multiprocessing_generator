# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

import sys
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
                get_timeout=10,
                n_jobs=1):
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

        self.processes = []
        for i in range(n_jobs):
            self.processes.append(Process(target=wrapped))
        self.processes_started = [False] * n_jobs

    def __enter__(self):
        """
        Starts the process
        """
        for i, process in enumerate(self.processes):
            process.start()
            self.processes_started[i] = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Kills the process
        """
        if self.processes:
            for process in self.processes:
                if process.is_alive():
                    process.terminate()

    def __next__(self):
        return self.next()

    def __iter__(self):
        return self

    def next(self):
        if not all(self.processes_started):
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
                    if not all([process.is_alive() for process in self.processes]):
                        raise GeneratorDied(
                            "The generator died unexpectedly.")

            if type(item) == ExceptionItem:
                raise item.exception
            return item

        except Exception:
            self.queue = None
            for process in self.processes:
                if process.is_alive():
                    process.terminate()
            self.processes = None
            raise


