#! /usr/bin/python

import sys
import math
import random

def main():
    if len(sys.argv) < 2:
        print ("Usage: " + sys.argv[0] + "[original delimiter] [percentage to keep]")
        sys.exit(1)
    
    delimiter = "\t"
    ratio = 1

    if len(sys.argv) == 3:
        delimiter = sys.argv[1]
        ratio = sys.argv[2]
    
    if len(sys.argv) == 2:
        ratio = sys.argv[1]


    for line in sys.stdin:
        # Split based on the original field delimiter
        sp = line.strip().split(delimiter)
        
        # Add your filters here
        # Example:
        #if len(sp[0]) < 0: # Skip object if the default id field is less than 0
        #    continue

        # Uniformly sample
        # if random.random() < float(ratio):
        print ("\t".join(sp))
            

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

