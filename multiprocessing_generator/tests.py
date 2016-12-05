# -*- encoding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from . import ParallelGenerator
from . import ParallelGeneratorException
from . import GeneratorDied
import datetime
import time
import itertools
import sys

class ParallelGeneratorTest(unittest.TestCase):
    def test_bad_init(self):
        """
        The generator cannot be used without
        invoking __enter__ via a with block
        """
        l = [1,2]
        with self.assertRaises(ParallelGeneratorException):
            pl = list(ParallelGenerator(l))

    def test_finite(self):
        """
        A finite list is fetched in a transparent way
        """
        l = [1,'a',None,0]
        with ParallelGenerator(l) as g:
            self.assertEqual(l, list(g))
    
    def test_exception(self):
        """
        Exceptions are passed to the outer generator
        """
        def mygen():
            for i in range(4):
                yield i
            raise ValueError('oops')
        
        with self.assertRaises(ValueError):
            with ParallelGenerator(mygen()) as g:
                l = list(g)

    def test_timing(self):
        """
        Test that items are fetched in the background
        """
        def slowgen():
            for i in range(10):
                time.sleep(1)
                yield i

        with ParallelGenerator(slowgen()) as g:
            # sleep while the parallel generator
            # fetches the first items.
            time.sleep(5)
            t1 = datetime.datetime.utcnow()
            # fetch the 4 first items from the
            # generator.
            for i in g:
                if i == 3:
                    break
            t2 = datetime.datetime.utcnow()
            self.assertTrue(
                t2-t1 < datetime.timedelta(seconds=1))

    def test_killed(self):
        """
        If the subprocess dies without finishing
        its generation, we want an exception
        to be raised.
        """
        def badgen():
            for i in range(10):
                if i == 5:
                    sys.exit(1)
                yield i

        with ParallelGenerator(badgen(), get_timeout=2) as g:
            with self.assertRaises(GeneratorDied):
                l = list(g)

    def test_gave_up(self):
        """
        If the consumer gives up, the process is killed
        """
        process = None
        with ParallelGenerator(itertools.count(), max_lookahead=10) as g:
            # dirty: we capture the inner process for later inspection
            process = g.process

            for i in g:
                if i >= 42:
                    # now we are bored
                    break

        time.sleep(1)
        self.assertFalse(process.is_alive())

