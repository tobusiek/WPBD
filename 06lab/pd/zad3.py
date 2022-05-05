import time
from functools import reduce
from multiprocessing import Pool, cpu_count
import sys


def mapper(letter):
    return [letter, 1]


def reducer(a, b):
    if type(a[0]) is not list: a = [a]
    if type(b[0]) is not list: b = [b]

    a_letters = [letter[0] for letter in a]
    a_idx = [idx for idx in range(len(a))]
    a_dict = dict(zip(a_letters, a_idx))

    for letter_b in b:
        if letter_b[0] in a_letters:
            a[a_dict[letter_b[0]]][1] += letter_b[1]
        else: a.append(letter_b)
            
    return a


def chunkify(data, n_chunks):
    return(data[i::n_chunks] for i in range(n_chunks))


def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk)
    return reduce(reducer, mapped_chunk)
    

n_cpus = cpu_count()

for f in range(len(sys.argv)-1):
    print('file', sys.argv[f+1])
    with open(sys.argv[f+1]) as f:
        text = f.read()
        text = list(text)

    start = time.process_time()
    data_chunks = chunkify(text, n_cpus)

    with Pool(processes=n_cpus) as pool:
        mapped = pool.map(chunks_mapper, data_chunks)
        reduced = reduce(reducer, mapped)

    for k, v in reduced:
        if k == '\n':
            print(fr'\n {v}')
        elif k == ' ':
            print(f'\' \' {v}')
        else:
            print(fr'{k} {v}')
    
    print(time.process_time() - start)
    print()
