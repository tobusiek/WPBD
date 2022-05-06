from mrjob.job import MRJob
from mrjob.step import MRStep
import re


WORD_RE = re.compile(r'[\w]+')


class MRWordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_word,
            combiner=self.combiner_word_count,
            reducer=self.reducer_word_count),
            MRStep(reducer=self.reducer_find_max_word)
        ]
    

    def mapper_get_word(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    
    def combiner_word_count(self, word, count):
        yield word, sum(count)
    

    def reducer_word_count(self, word, counts):
        yield None, (sum(counts), word)


    def reducer_find_max_word(self, _, word_count_pair):
        yield max(word_count_pair)


if __name__ == '__main__':
    MRWordCount.run()
