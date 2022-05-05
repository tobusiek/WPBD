from time import process_time
from functools import reduce
from multiprocessing import Pool, cpu_count


def clean_words(word):
    word = word.lower()
    word = word.replace(',', '')
    word = word.replace('.', '')
    word = word.replace('\\n', '')
    word = word.replace('\\t', '')
    word = word.replace('!', '')
    word = word.replace('?', '')
    if word.startswith('\''): word = word[1:]
    if word.endswith('\''): word = word[:-1]
    return word


def mapper(word):
    return [word, 1]


def reducer(a, b):
    if type(a[0]) is not list: a = [a]
    if type(b[0]) is not list: b = [b]

    a_words = [word[0] for word in a]
    a_idx = [idx for idx in range(len(a))]
    a_dict = dict(zip(a_words, a_idx))

    for word_b in b:
        if word_b[0] in a_words:
            a[a_dict[word_b[0]]][1] += word_b[1]
        else: a.append(word_b)
            
    return a


def chunkify(data, n_chunks):
    return(data[i::n_chunks] for i in range(n_chunks))


def chunks_mapper(chunk):
    chunk = map(clean_words, chunk)
    mapped_chunk = map(mapper, chunk)
    return reduce(reducer, mapped_chunk)
    

with open('./opowiesc.txt') as f:
    text = f.read()
    text = text.replace('–', ' ')
    text = text.strip().split()

start = process_time()
n_cpus = cpu_count()
data_chunks = chunkify(text, n_cpus)

with Pool(processes=n_cpus) as pool:
    mapped = pool.map(chunks_mapper, data_chunks)
    reduced = reduce(reducer, sorted(mapped))

print(sorted(reduced, key=lambda x:x[1], reverse=True))
print(process_time() - start)
