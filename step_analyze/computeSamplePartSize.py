#!/usr/bin/env python

import sys
import math


# Extract the bounding rectangles of the entire space
# The input should be tab-separated mbbs generated from step_sample
#   object_id  min_x min_y max_x max_y
# Note: the mbbs are not normalized

def main():
    if len(sys.argv) < 4:
        print("Not enough arguments. Usage: " + sys.argv[0] + " [blocksize] [sampleration] [avgObjSize]")
        sys.exit(1)
    blocksize = int(sys.argv[1])
    sampleratio = float(sys.argv[2])
    avgObjSize = float(sys.argv[3])
    print(int(math.floor(blocksize * sampleratio / avgObjSize)))

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

