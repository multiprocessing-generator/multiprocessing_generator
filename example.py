import time
from multiprocessing_generator import ParallelGenerator


def mock_batch_generator():
    while True:
        time.sleep(2)
        yield 'foo'


if __name__ == '__main__':

    print('Normal generator:')
    start = time.time()
    cnt = 0
    for elem in mock_batch_generator():
        if cnt == 5:
            break

        # mock training process
        time.sleep(1)

        print('Batch %s. Elapsed %s' % (cnt, time.time() - start))
        cnt += 1

    print('Parallel generator:')
    start = time.time()
    cnt = 0
    with ParallelGenerator(mock_batch_generator(), max_lookahead=10) as parallel_generator:
        for elem in parallel_generator:
            if cnt == 5:
                break

            # mock training process
            time.sleep(1)

            print('Batch %s. Elapsed %s' % (cnt, time.time() - start))
            cnt += 1

    print('Parallel generator with 2 threads:')
    start = time.time()
    cnt = 0
    with ParallelGenerator(mock_batch_generator(), max_lookahead=10, n_jobs=2) as parallel_generator:
        for elem in parallel_generator:
            if cnt == 5:
                break

            # mock training process
            time.sleep(1)

            print('Batch %s. Elapsed %s' % (cnt, time.time() - start))
            cnt += 1
