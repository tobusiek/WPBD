'''
Wykorzystując przykłady z zajęć napisz program w Pythonie
z zastosowaniem biblioteki mrjob, który policzy najwyższe
pensje w pliku salaries.csv i zwróci 10 najwyższych
(nie musi być malejąco ani rosnąco).
'''
from mrjob.job import MRJob


class MRWordCount(MRJob):
    def mapper(self, _, line):
        salary_str = line.strip().split(',')[-2]
        salary = float(salary_str.replace('$', ''))
        yield None, salary
    

    def reducer(self, _, salaries):
        top10 = list()

        for salary in salaries:
            top10.append(salary)
        
        top10.sort(reverse=True)
        top10 = top10[:10]

        for idx, salary in enumerate(top10):
            yield idx+1, salary


if __name__ == '__main__':
    MRWordCount.run()
