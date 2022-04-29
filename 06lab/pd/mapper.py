#!/usr/bin/env python
import sys


for line in sys.stdin:
    words = line.split()
    words = [word.strip() for word in words]
    words = [word.lower() for word in words]
    words = [word.replace(',', '') for word in words]
    words = [word.replace('.', '') for word in words]
    words = [word.replace('\\n', '') for word in words]
    words = [word.replace('\\t', '') for word in words]
    words = [word.replace('!', '') for word in words]
    words = [word.replace('?', '') for word in words]
    words = [word[1:] if word.startswith('\'') else word for word in words]
    words = [word[:-1] if word.endswith('\'') else word for word in words]

    for word in words:
        print(f'{word}\t{1}')
