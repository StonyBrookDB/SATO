#! /usr/bin/python

import sys

def cat():
    for line in sys.stdin:
        sys.stdout.write(line)

def uniq():
    prev = None
    for line in sys.stdin:
        line = line.strip()
        if line != prev:
            print line
            prev = line

def uniq2():
    prev = None
    for line in sys.stdin:
	line = line.strip()
        joinRes = line.rsplit('\t', 1)[0]
        if joinRes != prev:
            print line
            prev = joinRes

def sort():
    lines = []
    for line in sys.stdin:
        lines.append(line.strip())

    lines.sort()
    
    for line in lines:
        print line

def main():
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: "+ sys.argv[0] + "[ cat | sort | uniq | uniq2 ]\n")
        sys.exit(1)
    cmd = sys.argv[1].lower()

    if cmd == "cat":
        cat()
    elif cmd == "uniq":
        uniq()
    elif cmd == "sort":
        sort()
    elif cmd == "uniq2":
        uniq2()
    else:
        sys.stderr.write("Unknown option: [" + cmd + "]\n")

    sys.stdout.flush()

if __name__ == '__main__':
    main()
