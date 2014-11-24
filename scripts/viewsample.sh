#! /bin/bash

# Configuring lib and include directories
usage(){
  echo -e "filter.sh [options]\n \
  -p HDFS_PATH_PREFIX, --prefix=HDFS_PATH_PREFIX \t directory path to the include locations \n \
  -n NUMBER_OF_LINES, --numlines==NUMBER_OF_LINES \t The number of lines to display \
"
  exit 1
}

# Default empty values
datapath=""
destination=""
numlines=10

while : 
do
    case $1 in
        -h | --help | -\?)
          usage;
          exit 0
          ;;
        -p | --prefix)
          datapath=$2
          shift 2
          ;;
        --prefix=*)
          datapath=${1#*=}
          shift
          ;;
        -n | --numlines)
          numlines=$2
          shift 2
	  ;;
        --numlines=*)
          numlines=${1#*=}
          shift
          ;;
        --)
          shift
          break
          ;;
        -*)
          echo "Unknown option: $1" >&2
          shift
          ;;
        *) # Done
          break
          ;;
     esac
done

# Setting global variables
HJAR=${HADOOP_STREAMING_PATH}/hadoop-streaming.jar

# Load the SATO configuration file
source ../sato.cfg
LD_CONFIG_PATH=${LD_LIBRARY_PATH}:${SATO_LIB_PATH}


if [ ! "${datapath}" ]; then
     echo "Error: Missing path to the loaded data. See --help"
     exit 1
fi

PARTITION_FILE=partfile.idx
number=$(hdfs dfs -cat ${datapath}/${PARTITION_FILE} | head -n 1 | cut -f1)

INPUT_1=${datapath}/data/${number}/*

# Hide the partition ID field
hdfs dfs -cat ${INPUT_1} | head -n ${numlines} | cut -f 3-


