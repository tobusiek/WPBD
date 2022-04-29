from audioop import reverse
import time
from functools import reduce
from multiprocessing import Pool, cpu_count, Manager


n_cpus = cpu_count()
# word_dict = dict()


def clean_word(word):
    word = word.lower()
    word = word.replace(',', '')
    word = word.replace('.', '')
    word = word.replace('\\n', '')
    word = word.replace('\\t', '')
    word = word.replace('!', '')
    word = word.replace('?', '')
    if word.startswith('\''):
        word = word[1:]
    if word.endswith('\''):
        word = word[:-1]
    return word


def mapper(word):
    global word_dict

    if word not in word_dict:
        word_dict[word] = 1
    
    return word, 1


def reducer(a, b):
    global word_dict

    if a[0] == b[0]:
        count = a[1] + b[1]
        word_dict[a[0]] = word_dict[a[0]] + count
        return a
    return b


def chunkify(data, n_chunks):
    data = data.split()
    data = sorted(data)
    return (data[i:len(data):n_chunks] for i in range(n_chunks))


def chunks_mapper(chunk):
    global word_dict

    chunk = map(clean_word, chunk)
    mapped_chunk = map(mapper, chunk)
    return reduce(reducer, mapped_chunk)
    

with open('./opowiesc.txt', 'r') as f:
    text = f.read()
    text = text.split()
    text = [word.replace('–', ' ') if word.count('–') > 0 else word for word in text]
    text = " ".join(text)

start = time.process_time()
data_chunks = chunkify(text, n_cpus)


manager = Manager()
word_dict = manager.dict()

with Pool(processes=n_cpus) as pool:
    mapped = pool.map(chunks_mapper, data_chunks)
    reduced = reduce(reducer, mapped)

print(dict(sorted(word_dict.items(), key=lambda d:d[1], reverse=True)))
print(time.process_time() - start)
