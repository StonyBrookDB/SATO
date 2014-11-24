#! /bin/bash

# Configuring lib and include directories
usage(){
  echo -e "containmentQuery.sh [options]\n \
  -p HDFS_PATH_PREFIX, --prefix=HDFS_PATH_PREFIX \t HDFS prefix to the data set (used in the loaded step) \n \
  -d DESTINATION_RESULT_PATH, --destination=DESTINATION_RESULT_PATH \t The destination for the result data (HDFS path) \n \
  -i MIN_X, --min_x=MIN_X \t The smallest x-coordinate of the query window \n \
  -j MIN_Y, --min_y=MIN_Y \t The smallest y-coordinate of the query window \n \
  -k MAX_X, --max_x=MAX_X \t The largest x-coordinate of the query window \n \
  -l MAX_Y, --max_y=MAX_Y \t The largest y-coordinate of the query window. \
"
 # -i OBJECT_ID, --obj_id=OBJECT_ID \t The field (position) of the object ID \n \
  exit 1
}

# Default empty values
datapath=""
destination=""


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
        -i | --min_x)
          min_x=$2
          shift 2
	  ;;
        --min_x=*)
          min_x=${1#*=}
          shift
          ;;
        -j | --min_y)
          min_y=$2
          shift 2
	  ;;
        --min_y=*)
          min_y=${1#*=}
          shift
          ;;
        -k | --max_x)
          max_x=$2
          shift 2
	  ;;
        --max_x=*)
          max_x=${1#*=}
          shift
          ;;
        -l | --max_y)
          max_y=$2
          shift 2
	  ;;
        --max_y=*)
          max_y=${1#*=}
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
export LD_LIBRARY_PATH=${LD_CONFIG_PATH}

# The location of the index file
PARTITION_FILE=partfile.idx

PATH_RETRIEVER=../containment/getInputPath.py

if ! [ "${min_x}" ] && ! [ "${min_y}" ] && ! [ "${max_x}" ] && ! [ "${max_y}" ]; then
     echo "ERROR: Missing query window dimensions. See --help"
     exit 1
fi

if [ ! "${datapath}" ]; then
     echo "Error: Missing path to the loaded data. See --help"
     exit 1
fi

if [ ! "${destination}" ]; then
     echo "Error: Missing path for the result/destination. See --help"
     exit 1
fi

#TMP_PARTITION_FILE=$(mktemp)
TMP_INPUT_PATH=$(mktemp)

#echo "Creating "${TMP_PARTITION_FILE}
echo "Creating "${TMP_INPUT_PATH}

# rm ${TMP_PARTITION_FILE}
hdfs dfs -get ${datapath}/${PARTITION_FILE} ${TMP_PARTITION_FILE}

###
### Need a check for valid data + cfg file in the HDFS path
###

hdfs dfs -cat ${datapath}/${PARTITION_FILE} | ../containment/getInputPath.py ${min_x} ${min_y} ${max_x} ${max_y} ${datapath}/data/ > ${TMP_INPUT_PATH}
#../containment/getInputPath.py ${min_x} ${min_y} ${max_x} ${max_y} ${datapath}/data/ < ${TMP_PARTITION_FILE} > ${TMP_INPUT_PATH}

rm ${TMP_PARTITION_FILE}
num_input_files=`(wc -l "${TMP_INPUT_PATH}" | cut -d" " -f1)`

echo "Number of input paths: "${num_input_files}

if [ ${num_input_files} -eq 0 ]; then
   echo "No object lies within the selected query window"
   exit 0 
fi

# Load the configuration file to determine the geometry field
DATA_CFG_FILE=data.cfg
rm -f ${DATA_CFG_FILE}
hdfs dfs -get ${datapath}/${DATA_CFG_FILE} ./
source ${DATA_CFG_FILE}

echo ${TMP_INPUT_PATH}

MAPPER_1=containment
MAPPER_1_PATH=../joiner/containment
OUTPUT_1=${destination}

# Removing the destination directory
hdfs dfs -rm -f -r ${destination}


echo "Intersecting partitions found"
# There are at least 1 partition/block intersecting with the window query
if [[ ${num_input_files} == 1 ]] ; then
   echo "Reading only 1 partition. No MapReduce job will be invoked."
   file_name=`(cat "${TMP_INPUT_PATH}")`

   hdfs dfs -mkdir -p ${destination}
   hdfs dfs -cat ${file_name}/* |  ${MAPPER_1_PATH} ${min_x} ${min_y} ${max_x} ${max_y} ${geomid} > ${TMP_INPUT_PATH}
   hdfs dfs -put ${TMP_INPUT_PATH} ${OUTPUT_1}/part-00000
else
   echo "Invoking MapReduce."
   echo "Querying: $min_x $min_y $max_x $max_y"
   input_path=" "
   while read -r line
      do
        input_path=${input_path}"-input "${line}" "
   done < ${TMP_INPUT_PATH}
   echo "Input path: "${input_path}
   hadoop jar ${HJAR} ${input_path} -output ${destination} -file ${MAPPER_1_PATH} -mapper "${MAPPER_1} ${min_x} ${min_y} ${max_x} ${max_y} ${geomid}" -reducer None --cmdenv LD_LIBRARY_PATH=${LD_CONFIG_PATH} -numReduceTasks 0
fi

rm ${TMP_INPUT_PATH}
echo "Done. Results are available at ${OUTPUT_1}"

