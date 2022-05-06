'''
Przerób program, tak aby zwracał średnie zarobki
na każdym ze stanowisk.
'''
from mrjob.job import MRJob


class MRWordCount(MRJob):
    def mapper(self, _, line):
        args = line.strip().split(',')
        salary_str = args[-2]
        salary = float(salary_str.replace('$', ''))
        position = args[2]
        yield position, salary
    

    def reducer(self, position, salaries):
        salary_list = list()

        for salary in salaries:
            salary_list.append(salary)

        yield position, sum(salary_list)/len(salary_list)


if __name__ == '__main__':
    MRWordCount.run()