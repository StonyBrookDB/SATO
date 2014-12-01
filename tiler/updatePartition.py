#! /usr/bin/python

import sys
import math


def main():
   if (len(sys.argv) != 2):
        print("Not enough arguments. Usage: " + sys.argv[0] + " partition_file")
        sys.exit(1)
   nonemptyparts = []

   for line in sys.stdin:
       sp = line.strip().split("\t")
       count = int(sp[1])
       if count > 0:
           if not int(sp[0]) in nonemptyparts:
               nonemptyparts.append(int(sp[0]))
 

   for line in open(sys.argv[1], 'r'):
       sp = line.strip().split("\t")
       if int(sp[0]) in nonemptyparts:
           print line.strip()
   sys.stdout.flush()
   sys.stderr.flush()

if __name__ == '__main__':
    main()

