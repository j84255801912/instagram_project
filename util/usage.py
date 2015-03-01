import sys

def error(*args):
    sys.stderr.write(' '.join(map(str, args)) + '\n')
