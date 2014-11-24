#! /usr/bin/python

import sys
import math

# Generate the command line argument for MapReduce 
#  to perform the containment query
# Command line argument (query window): min_x min_y max_x max_y prefix
#                       
# Standard input: region MBRs (partfile)
# Output format: -input path_1 -input path_2 ... -input path_n

# Determines whether two rectangles (MBRs) intersect
def intersect(min_x, min_y, max_x, max_y,
              min_x2, min_y2, max_x2, max_y2):
    return not (min_x > max_x2 or max_x < min_x2 or
               min_y > max_y2 or max_y < min_y2)

def main():
    if (len(sys.argv) != 6):
        print("Not enough arguments. Usage: " + sys.argv[0] + " min_x min_y max_x max_y hdfs_prefix")
        sys.exit(1)
        
 
    w_min_x = float(sys.argv[1])
    w_min_y = float(sys.argv[2])
    w_max_x = float(sys.argv[3])
    w_max_y = float(sys.argv[4])
    prefix = sys.argv[5]

    str = ""


    for line in sys.stdin:
        sp = line.strip().split("\t")
        minx = float(sp[1])
#        minx = (minx - space_min_x) / space_x_span
        miny = float(sp[2])
#        miny = (miny - space_min_y) / space_y_span
        maxx = float(sp[3])
#        maxx = (maxx - space_min_x) / space_x_span
        maxy = float(sp[4])
#        maxy = (maxy - space_min_y) / space_y_span
        fileName = prefix + sp[0]

        if intersect(minx, miny, maxx, maxy, w_min_x, w_min_y, w_max_x, w_max_y):
            print(fileName)
		#str += " -input " + fileName + " "
        #str.append(" -input ").append(fileName)
    
    #sys.stdout.write(str)

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == '__main__':
    main()

