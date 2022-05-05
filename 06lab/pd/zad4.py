from time import process_time
from functools import reduce
from multiprocessing import Pool, cpu_count
import numpy as np


def mapper(number):
    return [number, 1]


def reducer(a, b):
    if type(a[0]) is not list:
        reduced_a = [a]
        not_reduced_a = [a[0]]
    else:
        reduced_a = a[0]
        not_reduced_a = a[1]

    if type(b[0]) is not list:
        reduced_b = [b]
        not_reduced_b = [b[0]]
    else:
        reduced_b = b[0]
        not_reduced_b = b[1]

    a_numbers = [number[0] for number in reduced_a]
    a_idx = [idx for idx in range(len(reduced_a))]
    a_dict = dict(zip(a_numbers, a_idx))

    for number_b in reduced_b:
        if number_b[0] in a_numbers:
            number_a = reduced_a[a_dict[number_b[0]]]
            number_a[1] += number_b[1]
        else: reduced_a.append(number_b)
    
    for number_b in not_reduced_b:
        not_reduced_a.append(number_b)

    max_number = np.max(not_reduced_a)
    mean = np.mean(not_reduced_a)
    gmean = np.exp(np.log(not_reduced_a).mean())
    median = np.median(not_reduced_a)
    result = {'max':max_number, 'mean':mean, 'gmean':gmean, 'median':median}
            
    return [reduced_a, not_reduced_a, result]


def chunkify(data, n_chunks):
    return (data[i::n_chunks] for i in range(n_chunks))


def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk)
    return reduce(reducer, mapped_chunk)


numbers = np.random.randint(low=1, high=101, size=100000)

start = process_time()
n_cpus = cpu_count()
data_chunks = chunkify(numbers, n_cpus)

with Pool(processes=n_cpus) as pool:
    mapped = pool.map(chunks_mapper, data_chunks)
    reduced, not_reduced, result = reduce(reducer, mapped)
    print('biggest:', result['max'])
    print('mean:', result['mean'])
    print('geometric mean:', result['gmean'])
    print('med:', result['median'])
    print('numbers count:', dict(sorted(reduced)))

print(process_time() - start)
