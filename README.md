multiprocessing\_generator
==========================
[![Python tests](https://github.com/wetneb/pynif/actions/workflows/ci.yml/badge.svg)](https://github.com/wetneb/pynif/actions/workflows/ci.yml) [![PyPI](https://img.shields.io/pypi/v/multiprocessing-generator.svg)](https://pypi.python.org/pypi/multiprocessing-generator)

A library to prefetch items from a Python generator in the background,
using a separate process.

Install (no dependencies):

    pip install multiprocessing_generator

Example:
```python
    from multiprocessing_generator import ParallelGenerator

    def my_generator():
        while True:
	        # ... download something long ...
	        yield result

    with ParallelGenerator(
	   my_generator(),
	   max_lookahead=100) as g:
         for elem in g:
              # ... do some heavy processing on that element ...
```

Up to 100 elements ahead of what is consumed will be fetched by the generator
in the background, which is useful when the producer and the consumer do
not use the same resources (for instance network vs. CPU).

The generator handles exceptions and more serious failures transparently.

Distributed under the MIT license.

See also: https://github.com/justheuristic/prefetch_generator
