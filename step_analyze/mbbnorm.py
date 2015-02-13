#!/usr/bin/env python

import sys
import math
import random

# Normalize the mbb into the range [0, 1] where the original dimensions
#     are obtained via command line arguments
# The input should be tab-separated mbbs generated from step_sample
#   object_id  min_x min_y max_x max_y
# The output follows the same format as the input

def main():
    if len(sys.argv) < 5:
       print("Not enough argument. Usage: " + sys.argv[0] + "min_x min_y max_x max_y ")
       sys.exit(1)

    space_min_x = float(sys.argv[1])
    space_min_y = float(sys.argv[2])
    space_max_x = float(sys.argv[3])
    space_max_y = float(sys.argv[4])
    space_x_span = space_max_x - space_min_x
    space_y_span = space_max_y - space_min_y
    if len(sys.argv) == 6:
        sample_ratio = float(sys.argv[5])

    for line in sys.stdin:
        sp = line.strip().split("\t")
        minx = float(sp[1])
        minx = (minx - space_min_x) / space_x_span

        miny = float(sp[2])
        miny = (miny - space_min_y) / space_y_span

        maxx = float(sp[3])
        maxx = (maxx - space_min_x) / space_x_span

        maxy = float(sp[4])
        maxy = (maxy - space_min_y) / space_y_span

        print ("\t".join((sp[0], str(minx), str(miny), str(maxx), str(maxy))))

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

