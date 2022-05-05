from time import process_time
from functools import reduce
from multiprocessing import Pool, cpu_count
from numpy.random import randint
import numpy as np


def gmean1(x):
    return np.exp(np.log(x).mean())


def gmean2(numbers, count):
    numbers = np.array(numbers)
    res = numbers.prod()**(1./count)
    return res


def mapper(number):
    return [number, 1, number]


def reducer(a, b):
    if type(a[0]) is not list: new_a = [a]
    else: new_a = a[0]

    if type(b[0]) is not list: new_b = [b]
    else: new_b = b[0]

    a_numbers = [number[0] for number in new_a]
    a_idx = [idx for idx in range(len(new_a))]
    a_dict = dict(zip(a_numbers, a_idx))

    for number_b in new_b:
        if number_b[0] in a_numbers:
            number_a = new_a[a_dict[number_b[0]]]
            number_a[1] += number_b[1]
            number_a[2] = int(number_a[0]*number_a[1])
        else: new_a.append(number_b)
    
    max_number = max(new_a)[0]

    sum_numbers = sum(int(i[0]*i[1]) for i in new_a)
    count_numbers = sum(i[1] for i in new_a)
    mean = sum_numbers/count_numbers

    sum_log = np.log([int(i[0]*i[1]) for i in new_a])
    gmean = gmean2(sum_numbers, count_numbers)
    # mul_numbers = [i[2] for i in new_a]
    
    # gmean = np.power(mul_numbers, 1./len(mul_numbers)).mean()
    # gmean = 0
    
    sorted_a = sorted(new_a)
    a_length = len(new_a)
    med_idx = a_length//2
    if a_length%2 == 0: median = (sorted_a[med_idx][0]+sorted_a[-med_idx][0])//2
    else: median = sorted_a[med_idx][0]

    a = [new_a, {'max':max_number, 'mean':mean, 'gmean':gmean, 'median':median}]
            
    return a


def chunkify(data, n_chunks):
    return (data[i::n_chunks] for i in range(n_chunks))


def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk)
    return reduce(reducer, mapped_chunk)


numbers = randint(low=1, high=101, size=100000)
# numbers = [i for i in range(1, 10)]

start = process_time()
n_cpus = cpu_count()
data_chunks = chunkify(numbers, n_cpus)

with Pool(processes=n_cpus) as pool:
    mapped = pool.map(chunks_mapper, data_chunks)
    reduced, result = reduce(reducer, mapped)

    print('biggest:', result['max'], np.max(numbers))
    print('mean:', result['mean'], np.mean(numbers))
    print('geometric mean:', result['gmean'], gmean1(numbers))
    print('med:', result['median'], np.median(numbers))
    unique_numbers = dict(sorted([(i[0], i[1]) for i in reduced]))
    # print('numbers count:', unique_numbers)

print(process_time() - start)


'''
ile zalozyc ze bedzie liczb? 100k? jaki przedzial wartosci liczb? - mozna zalozyc jakis przedzial
czy mozna uzywac bibliotek numpy/statistics/math itd? tez funkcje wbudowane - max, sorted - mozna uzywac gotowych funkcji nawet mean czy median
czy ma byc wyslane sprawozdanie czy pliki z kodem do zadan? - pliki z kodem i wyniki wywolan
czy wszystkie podpunkty maja byc w jednym pliku - obojetnie
'''