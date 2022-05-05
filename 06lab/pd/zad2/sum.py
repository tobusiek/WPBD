import sys


sum = 0
for line in sys.stdin:
    _, count = line.split('\t')
    sum += int(count)

print(sum)