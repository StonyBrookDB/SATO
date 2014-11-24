#! /bin/bash

# Configuring lib and include directories
usage(){
  echo -e "spatialjoin.sh [options]\n \
  -a PATH_TO_DATA_1, --inputa=PATH_TO_DATA_1 \t The HDFS prefix path to the loaded data set 1 \n \
  -b PATH_TO_DATA_2, --inputb=PATH_TO_DATA_2 \t The HDFS prefix path to the loaded data set 2 \n \
  -p PREDICATE, --predicate=join predicate \t The join predicate [contains | intersects | touches | crosses | within | dwithin] \n \
  -q DISTANCE, --qdistance=DISTANCE \t Distance (for dwithin predicate) \n \
  -d DESTINATION_PATH, --destination=DESTINATION_PATH\tThe HDFS prefix path to store the query result \n \
  -f FIELDS_TO_OUTPUT, --fields=FIELDS_TO_OUTPUT \t The fields to be included in the outputs. The format is comma-separated for fields within 1 data set, and separated by color between datasets. E.g. --fields=1,3,4:2,5,9 \n \
  -n NUMBER_REDUCERS, --num_reducers=NUM_REDUCERS \t Number of reducers to be used \n \
  -s TRUE_OR_FALSE, --statistics=TRUE_OR_FALSE \t Appending additional spatial join statistics to joined pairs: [true | false]. \n \
  -m PARTITION_METHOD, --method=PARTITION_METHOD \t OPTIONAL - The partitioning method. The default method is fixed grid partitioning. [ fg | bsp ] \n \
   -r SAMPLING_RATIO, --ratio=SAMPLING_RATIO \t OPTIONAL - The sampling ratio for partitioning the data. Default value is 1.0."
 # -i OBJECT_ID, --obj_id=OBJECT_ID \t The field (position) of the object ID \n \
  exit 1
}

# Setting global variables
HJAR=${HADOOP_STREAMING_PATH}/hadoop-streaming.jar
bucketsize=5000
SATO_CONFIG_FILE_NAME=data.cfg
SATO_INDEX_FILE_NAME=partfile.idx

# Default empty values
prefixpath1=""
prefixpath2=""
geomid=""
delimiter=""
predicate=""
sample_ratio=1
method="fg"
statistics="false"
num_reducers=""
qdistance=0
fields=""

while : 
do
    case $1 in
        -h | --help | -\?)
          usage;
          exit 0
          ;;
        -a | --inputa)
          prefixpath1=$2
          shift 2
          ;;
        --inputa=*)
          prefixpath1=${1#*=}
          shift
          ;;
        -b | --inputb)
          prefixpath2=$2
          shift 2
          ;;
        --inputb=*)
          prefixpath2=${1#*=}
          shift
          ;;
        -d | --destination)
          destination=$2
          shift 2
          ;;
        --destination=*)
          destination=${1#*=}
          shift
          ;;
        -p | --predicate)
          predicate=$2
          shift 2
          ;;
        --predicate=*)
          predicate=${1#*=}
          shift
          ;;
        -q | --qdistance)
          qdistance=$2
          shift 2
          ;;
        --qdistance=*)
          qdistance=${1#*=}
          shift
          ;;
        -n | --num_reducers)
          num_reducers=$2
          shift 2
          ;;
        --num_reducers=*)
          num_reducers=${1#*=}
          shift
          ;;
        -s | --statistics)
          statistics=$2
          shift 2
          ;;
        --separator=*)
          statistics=${1#*=}
          shift
          ;;
        -r | --ratio)
          sample_ratio=$2
          shift 2
          ;;
        --ratio=*)
          sample_ratio=${1#*=}
          shift
          ;;
        -m | --method)
          method=$2
          shift 2
          ;;
        --method=*)
          method=${1#*=}
          shift
          ;;
        -f | --fields)
          fields=$2
          shift 2
          ;;
        --fields=*)
          fields=${1#*=}
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

SATO_CONFIG=../sato.cfg
# Load the SATO configuration file
if [ -e "${SATO_CONFIG}" ]; then
  source ${SATO_CONFIG}
else
  echo "SATO configuration file not found!"
fi

LD_CONFIG_PATH=${LD_LIBRARY_PATH}:${SATO_LIB_PATH}
export LD_LIBRARY_PATH=${LD_CONFIG_PATH}

if [ ! "${prefixpath1}" ] || [ ! "${prefixpath2}" ]; then
  echo "ERROR: Missing path to input data sets. See --help" >&2
  exit 1
fi
if [ ! "${destination}" ]; then
  echo "ERROR: Missing path to the destination (result). See --help" >&2
  exit 1
fi

if [ "${predicate}" != "intersects" ] && [ "${predicate}" != "touches" ] && [ "${predicate}" != "crosses" ] && [ "${predicate}" != "contains" ] && [ "${predicate}" != "adjacent" ] && [ "${predicate}" != "disjoint" ] && [ "${predicate}" != "equals" ] && [ "${predicate}" != "dwithin" ] && [ "${predicate}" != "within" ] && [ "${predicate}" != "overlaps" ]; then
   echo "ERROR: Invalid predicate. See --help" >&2
   exit 1
fi

if ! [[ ${num_reducers} -ge 1 ]]; then
  echo "ERROR: Missing the number of reducers. See --help" >&2
  exit 1
fi

if ! [ "${method}" == "fg" ] && ! [ "${method}" == "bsp" ] ; then
   echo "Invalid partitioning method"
   exit 1
fi

# Creating the path with the HDFS prefix
hdfs dfs -mkdir -p ${destination}

# To be added: Check if prefixpath1 and prefixpath2 contain valid data and configuration file

# Remove the output directory

echo "Combining config files"

# The object MBRs are located in ${prefixpath1}/mbb and ${prefixpath2}/mbb
# The next step will obtain the overall space dimension

# Determine the min, max dimensions of the combined space
TEMP_CFG_FILE=tmpcfgfile
echo ${prefixpath1}
echo ${prefixpath2}

#cat <( hdfs dfs -cat "${prefixpath1}"/data.cfg ) <( hdfs dfs -cat "${prefixpath2}"/data.cfg )

cat <( hdfs dfs -cat ${prefixpath1}/data.cfg ) <( hdfs dfs -cat ${prefixpath2}/data.cfg ) | ../step_analyze/combineCfg.py > ${TEMP_CFG_FILE} 

# cat ${TEMP_CFG_FILE}

# rm ${TEMP_CFG_FILE}

source ${TEMP_CFG_FILE}

# Normalize the space using the dimension obtained from above

min_x=${dataminx}
min_y=${dataminy}
max_x=${datamaxx}
max_y=${datamaxy}

# Outputting the space dimensions
echo ${min_x}
echo ${min_y}
echo ${max_x}
echo ${max_y}
echo ${numobjects}
echo ${geomid1}
echo ${geomid2}

TEMP_CFG_FILE=joinconfig

# Write the config file
echo "dataminx=${min_x}" > ${TEMP_CFG_FILE}
echo "dataminy=${min_y}" >> ${TEMP_CFG_FILE}
echo "datamaxx=${max_x}" >> ${TEMP_CFG_FILE}
echo "datamaxy=${max_y}" >> ${TEMP_CFG_FILE}
echo "numobjects=${numobjects}" >> ${TEMP_CFG_FILE}
echo "geomid1=${geomid1}" >> ${TEMP_CFG_FILE}
echo "geomid2=${geomid2}" >> ${TEMP_CFG_FILE}


TEMP_DIR=${destination}_tmp

# Normalize the mbbs
INPUT_1A=${prefixpath1}/mbb
INPUT_1B=${prefixpath2}/mbb
OUTPUT_1=${TEMP_DIR}
MAPPER_1=mbbnorm.py
MAPPER_1_PATH=../step_analyze/mbbnorm.py

hdfs dfs -rm -f -r ${OUTPUT_1}

echo "Normalizing MBBs"
# Normalizing only MBB of set 2
hadoop jar ${HJAR} -input ${INPUT_1B} -output ${OUTPUT_1} -file ${MAPPER_1_PATH} -mapper "${MAPPER_1} ${min_x} ${min_y} ${max_x} ${max_y}" -reducer None -numReduceTasks 0

if [  $? -ne 0 ]; then
   echo "Normalizing MBB has failed!"
   exit 1
fi

# Determine the optimal bucket count
partitionSize=20

echo "partitionsize=${partitionSize}"

#INPUT_MBB_FILE=mbbnormfile
INPUT_MBB_FILE="$(mktemp)"

#PARTITION_FILE=partfile
PARTITION_FILE="$(mktemp)"

hdfs dfs -cat "${OUTPUT_1}/*" > ${INPUT_MBB_FILE}

echo "Start partitioning"

# Partition data
if [ "$method" == "fg" ]; then
   ../step_tear/fg/serial/fgNoMbb.py ${min_x} ${min_y} ${max_x} ${max_y} ${partitionSize} ${numobjects} > ${PARTITION_FILE}
fi

if [ "$method" == "bsp" ]; then
   ../step_tear/bsp/serial/bsp -b ${partitionSize} -i ${INPUT_MBB_FILE} > ${PARTITION_FILE}
fi
cat ${PARTITION_FILE}
echo "Done partitioning"

# Remove temporary files
rm ${INPUT_MBB_FILE}

PARTITION_FILE_DENORM=partfiledenorm

# Denormalize the MBB file and copy them to HDFS
python ../step_tear/denormalize.py ${min_x} ${min_y} ${max_x} ${max_y}  < ${PARTITION_FILE} > ${PARTITION_FILE_DENORM}


rm ${PARTITION_FILE}
cp ${PARTITION_FILE_DENORM} ${SATO_INDEX_FILE_NAME}
hdfs dfs -put ${PARTITION_FILE_DENORM} ${OUTPUT_1}/${SATO_INDEX_FILE_NAME}

INPUT_2A=${prefixpath1}'/data/*/*'
INPUT_2B=${prefixpath2}'/data/*/*'
OUTPUT_2=${destination}_tmp2
MAPPER_2=partitionMapperJoin
MAPPER_2_PATH=../tiler/partitionMapperJoin
REDUCER_2=resque
REDUCER_2_PATH=../joiner/resque

hdfs dfs -rm -f -r ${OUTPUT_2}


hdfs dfs -rm -f -r ${OUTPUT_1}
predicate="st_"${predicate}

echo "${MAPPER_2} ${geomid1} ${geomid2} ${SATO_INDEX_FILE_NAME} ${prefixpath1} ${prefixpath2}"
echo "${REDUCER_2} -p ${predicate} -i ${geomid1} -j ${geomid2} -s ${statistics}"


#Perform spatial join
hadoop jar ${HJAR} -input ${INPUT_2A} -input ${INPUT_2B} -output ${OUTPUT_2} -file ${MAPPER_2_PATH} -file ${REDUCER_2_PATH} -file ${SATO_INDEX_FILE_NAME}  -mapper "${MAPPER_2} ${geomid1} ${geomid2} ${SATO_INDEX_FILE_NAME} ${prefixpath1} ${prefixpath2}" -reducer "${REDUCER_2} -p ${predicate} -i ${geomid1} -j ${geomid2} -s ${statistics} -d ${qdistance} -f ${fields} " -cmdenv LD_LIBRARY_PATH=${LD_CONFIG_PATH} -numReduceTasks ${num_reducers}

if [  $? -ne 0 ]; then
   echo "Spatial computation has failed!"
   exit 1
fi

rm -f ${SATO_INDEX_FILE_NAME}
rm -f ${PARTITION_FILE_DENORM}

INPUT_3=${OUTPUT_2}
OUTPUT_3=${destination}
REDUCER_3=hgdeduplicater.py
REDUCER_3_PATH=../joiner/hgdeduplicater.py
hdfs dfs -rm -r ${OUTPUT_3}
echo -e "Deduplication step" 

hadoop jar ${HJAR} -file ${REDUCER_3_PATH} -mapper 'cat -' -reducer "${REDUCER_3} uniq" -input ${INPUT_3} -output ${OUTPUT_3} -numReduceTasks ${num_reducers} -jobconf mapred.task.timeout=360000000

succ=$?

hdfs dfs -rm -r ${OUTPUT_2}
if [[ $succ != 0 ]] ; then
    echo -e "\n\n deduplication stage has failed. \nPlease check the output result for debugging."
    exit $succ
fi

echo "Done. Results are available at ${OUTPUT_3}"
