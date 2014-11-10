#! /bin/bash

# Configuring lib and include directories
usage(){
  echo -e "filter.sh [options]\n \
  -p HDFS_PATH_PREFIX, --prefix=HDFS_PATH_PREFIX \t directory path to the include locations \n \
  -d DESTINATION_RESULT_PATH, --destination=DESTINATION_RESULT_PATH \t The destination for the result data (HDFS path) \n \
  -f pythonfile \t The name of the custom python function \n \
"
  exit 1
}

# Default empty values
datapath=""
destination=""
file=""

while : 
do
    case $1 in
        -h | --help | -\?)
          usage;
          exit 0
          ;;
        -d | --destination)
          destination=$2
          shift 2
          ;;
        --destination=*)
          destination=${1#*=}
          shift
          ;;
        -p | --prefix)
          datapath=$2
          shift 2
          ;;
        --prefix=*)
          datapath=${1#*=}
          shift
          ;;
        -f | --file)
          pythonfile=$2
          shift 2
	  ;;
        --file=*)
          pythonfile=${1#*=}
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
     echo "Error: Missing path to the loaded data"
     exit 1
fi

if [ ! "${destination}" ]; then
     echo "Error: Missing path for the result/destination"
     exit 1
fi

if [ ! "${destination}" ]; then
     echo "Error: Missing path for the result/destination"
     exit 1
fi

# Load the configuration file to determine the geometry field
DATA_CFG_FILE=data.cfg
rm -f ${DATA_CFG_FILE}
hdfs dfs -get ${datapath}/${DATA_CFG_FILE} ./
source ${DATA_CFG_FILE}


MAPPER_1=filter.py
MAPPER_1_PATH=../filters/filter.py
INPUT_1=${datapath}'/data/*/*'
OUTPUT_1=${destination}

# Removing the destination directory
hdfs dfs -rm -f -r ${OUTPUT_1}

hadoop jar ${HJAR} ${INPUT_1} -output ${OUTPUT_1} -file ${MAPPER_1_PATH} -mapper "${MAPPER_1} ${geomid} ${pythonfile}" -reducer None --cmdenv LD_LIBRARY_PATH=${LD_CONFIG_PATH} -numReduceTasks 0

echo "Done. Results are available at ${destination}"

