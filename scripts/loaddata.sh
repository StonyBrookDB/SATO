#! /bin/bash

# Configuring lib and include directories
usage(){
  echo -e "loaddata.sh [options]\n \
  -d PATH_TO_DATA, --data=PATH_TO_DATA \t The HDFS path to the raw data \n \
  -p HDFS_PATH_PREFIX, --prefix=HDFS_PATH_PREFIX \t directory path to the include locations \n \
  -g GEOM_ID, --geomid=GEOM_ID \t The field (position) of the geometry field (starts from 1) \n \
  -s SEPARATOR, --separator=SEPARATOR \t OPTIONAL - The seperator/delimiter used to separate fields in the original dataset. The default value is tab. \n \
  -r SAMPLING_RATIO, --ratio=SAMPLING_RATIO \t OPTIONAL - The sampling ratio to be used to partition data. Default value is 1.0. \n \
  -m PARTITION_METHOD, --method=PARTITION_METHOD \t OPTIONAL - The partitioning method. The default method is fixed grid partitioning. Options include: fg (fixed grid), bsp (binary space partitioning), sfc (space filling curve) \n \
  -n NUMBER_REDUCERS, --numreducers=NUMBER_REDUCERS \t OPTIONAL - The number of reducers to be used \n \
  -b BLOCK_SIZE, --blocksize=BLOCK_SIZE \t OPTIONAL - The block size in bytes (the default block size is 64MB).\
"
 # -i OBJECT_ID, --obj_id=OBJECT_ID \t The field (position) of the object ID \n \
  exit 1
}

# Setting global variables
HJAR=${HADOOP_STREAMING_PATH}/hadoop-streaming.jar
# default block size (partition size)
blocksize=67108864
SATO_CONFIG_FILE_NAME=data.cfg
SATO_INDEX_FILE_NAME=partfile.idx

# Default empty values
datapath=""
prefixpath=""
geomid=""
delimiter=""
sampleratio=1
method="fg"
numreducers=20

while : 
do
    case $1 in
        -h | --help | -\?)
          usage;
          exit 0
          ;;
        -d | --data)
          datapath=$2
          shift 2
          ;;
        --data=*)
          datapath=${1#*=}
          shift
          ;;
        -p | --prefix)
          prefixpath=$2
          shift 2
          ;;
        --prefix=*)
          prefixpath=${1#*=}
          shift
          ;;
        -g | --geomid)
          geomid=$2
          shift 2
          ;;
        --geomid=*)
          geomid=${1#*=}
          shift
          ;;
        -i | --obj_id)
          obj_id=$2
          shift 2
          ;;
        --obj_id=*)
          obj_id=${1#*=}
          shift
          ;;
        -s | --separator)
          delimiter=$2
          shift 2
          ;;
        --separator=*)
          delimiter=${1#*=}
          shift
          ;;
        -r | --ratio)
          sampleratio=$2
          shift 2
          ;;
        --ratio=*)
          sampleratio=${1#*=}
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
        -n | --numreducers)
          numreducers=$2
          shift 2
          ;;
        --numreducers=*)
          numreducers=${1#*=}
          shift
          ;;
        -b | --blocksize)
          blocksize=$2
          shift 2
          ;;
        --blocksize=*)
          blocksize=${1#*=}
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

if [ ! "$datapath" ]; then
  echo "ERROR: Missing path to the data. See --help" >&2
  exit 1
fi
if [ ! "$prefixpath" ]; then
  echo "ERROR: Missing the target HDFS prefix path. See --help" >&2
  exit 1
fi

if [ ! "$geomid" ]; then
  echo "ERROR: Missing the geometry id (field number). See --help" >&2
  exit 1
fi

if ! [ "$method" == "fg" ]  && ! [ "$method" == "bsp" ]; then
   echo "Invalid partitioning method"
   exit 1
fi


# Creating the path with the HDFS prefix
hdfs dfs -rm -r -f ${prefixpath}
hdfs dfs -mkdir -p ${prefixpath}

INPUT_1=${datapath}
OUTPUT_1=${prefixpath}/sampledtsv
MAPPER_1=samplefilter.py
MAPPER_1_PATH=../step_sample/${MAPPER_1}


# Remove the output directory
hdfs dfs -rm -f -r ${OUTPUT_1}
echo "Starting the sampling/filtering step"

# Sample / Filter step:
# This step will convert the original data into a tab-separated format (tsv file).
hadoop jar ${HJAR} -input ${INPUT_1} -output ${OUTPUT_1} -file ${MAPPER_1_PATH} -mapper "${MAPPER_1} ${delimiter} 1" -reducer None -numReduceTasks 0

if [  $? -ne 0 ]; then
   echo "Data conversion has failed!"
   exit 1
fi

echo "Finished the filtering/sampling step"


# Extract the mbbs from spatial objects
INPUT_2=${OUTPUT_1}
OUTPUT_2=${prefixpath}/mbb
MAPPER_2=mbbextractor
MAPPER_2_PATH=../tiler/mbbextractor

# Remove the output directory
hdfs dfs -rm -f -r ${OUTPUT_2}

# This is out-dated
# Optional depending on whether the sampling ratio is 1.0
#if [ "${sampleratio}" -lt 1 ]; then
#    # perform sampling
#    hadoop jar ${HJAR} -input ${INPUT_1} -output ${OUTPUT_1} -file ${MAPPER_1_PATH} -mapper "${MAPPER_1} ${delimiter} ${sampleratio}" -reducer None -numReduceTasks 0
#fi

echo "Extracting MBRs from objects"
hadoop jar ${HJAR} -input ${INPUT_2} -output ${OUTPUT_2} -file ${MAPPER_2_PATH} -mapper "${MAPPER_2} ${geomid} ${sampleratio}" -reducer None -cmdenv LD_LIBRARY_PATH=${LD_CONFIG_PATH} -numReduceTasks 0

if [ $? -ne 0 ]; then
   echo "Extracting MBRs has failed!"
   exit 1
fi

echo "Done extracting object MBRs"

# Determine the min, max dimensions of the space
INPUT_3=${OUTPUT_2}
OUTPUT_3=${prefixpath}/mbbstat
MAPPER_3=getSpaceDimension.py
MAPPER_3_PATH=../step_analyze/getSpaceDimension.py
REDUCER_3=${MAPPER_3}

echo "Retrieving space dimension"
# Remove the output directory
hdfs dfs -rm -f -r ${OUTPUT_3}
hadoop jar ${HJAR} -input ${INPUT_3} -output ${OUTPUT_3} -file ${MAPPER_3_PATH} -mapper "${MAPPER_3} 1" -reducer "${MAPPER_3} 0" -numReduceTasks 1

# Normalize the space using the dimension obtained from above
# TEMP_FILE_NAME=tmpSpaceDimension
#rm $TEMP_FILE_NAME
#create a temporary file
# TEMP_FILE_NAME="$(mktemp)"
read min_x min_y max_x max_y num_objects <<< `(hdfs dfs -cat ${OUTPUT_3}/part-00000)`

#rm -f ${TEMP_FILE_NAME}
hdfs dfs -rm -f -r ${OUTPUT_3}

TEMP_CFG_FILE="$(mktemp)"

# Outputting the space dimensions
echo ${min_x}
echo ${max_x}
echo ${min_y}
echo ${max_y}

# Write the config file
echo "dataminx=${min_x}" > ${TEMP_CFG_FILE}
echo "dataminy=${min_y}" >> ${TEMP_CFG_FILE}
echo "datamaxx=${max_x}" >> ${TEMP_CFG_FILE}
echo "datamaxy=${max_y}" >> ${TEMP_CFG_FILE}
echo "numobjects=${num_objects}" >> ${TEMP_CFG_FILE}
echo "geomid=${geomid}" >> ${TEMP_CFG_FILE}

# Normalize the mbbs
INPUT_4=${OUTPUT_2}
OUTPUT_4=${prefixpath}/mbbnorm
MAPPER_4=mbbnorm.py
MAPPER_4_PATH=../step_analyze/mbbnorm.py

if ! [ "$method" == "fg" ]; then
     hdfs dfs -rm -f -r ${OUTPUT_4}

     echo "Normalizing MBBs"
     hadoop jar ${HJAR} -input ${INPUT_4} -output ${OUTPUT_4} -file ${MAPPER_4_PATH} -mapper "${MAPPER_4} ${min_x} ${min_y} ${max_x} ${max_y}" -reducer None -numReduceTasks 0
     if [  $? -ne 0 ]; then
          echo "Normalizing MBB has failed!"
          exit 1
     fi
fi



# Determine the optimal bucket count
totalSize=`(hdfs dfs -du -s "${datapath}" | cut -d\  -f1)`
echo "Total size in bytes: "${totalSize}
echo "Number of objects: "${num_objects}
avgObjSize=$((totalSize / num_objects))

# partitionSize=$((blocksize * sampleratio / avgObjSize))
partitionSize=`(../step_analyze/computeSamplePartSize.py ${blocksize} ${sampleratio} ${avgObjSize})`

echo "partitionsize=${partitionSize}" >> ${TEMP_CFG_FILE}

# Copy the config file into HDFS
hdfs dfs -put ${TEMP_CFG_FILE} ${prefixpath}/${SATO_CONFIG_FILE_NAME}
rm -f ${TEMP_CFG_FILE}

INPUT_MBB_FILE="$(mktemp)"
#INPUT_MBB_FILE=mbbnormfile

#PARTITION_FILE=partfile

PARTITION_FILE="$(mktemp)"

hdfs dfs -cat "${OUTPUT_4}/*" > "${INPUT_MBB_FILE}"


echo "Start partitioning"

# Partition data
if [ "$method" == "fg" ]; then
   ../step_tear/fg/serial/fgNoMbb.py ${min_x} ${min_y} ${max_x} ${max_y} ${partitionSize} ${num_objects} > ${PARTITION_FILE}
fi

if [ "$method" == "bsp" ]; then
   ../step_tear/bsp/serial/bsp -b ${partitionSize} -i ${INPUT_MBB_FILE} > ${PARTITION_FILE}
fi

echo "Done partitioning"

# Remove temporary files

rm ${INPUT_MBB_FILE}

hdfs dfs -rm -f -r ${OUTPUT_4}

PARTITION_FILE_DENORM=partfiledenorm
# Denormalize the MBB file and copy them to HDFS
python ../step_tear/denormalize.py ${min_x} ${min_y} ${max_x} ${max_y}  < ${PARTITION_FILE} > ${PARTITION_FILE_DENORM}
# Copy the partition region mbb file onto HDFS
rm ${PARTITION_FILE}
cp ${PARTITION_FILE_DENORM} ${SATO_INDEX_FILE_NAME}


INPUT_5=${OUTPUT_1}
OUTPUT_5=${prefixpath}/data
MAPPER_5=partitionMapper
MAPPER_5_PATH=../tiler/partitionMapper
REDUCER_5=hgdeduplicater.py
REDUCER_5_PATH=../joiner/hgdeduplicater.py

hdfs dfs -rm -f -r ${OUTPUT_5}

echo "Mapping data to create physical partitions"
#Map the data back to its partition
hadoop jar ${HJAR} -libjars ../libjar/customLibs.jar -outputformat com.custom.CustomMultiOutputFormat  -input ${INPUT_1} -output ${OUTPUT_5} -file ${MAPPER_5_PATH} -file ${REDUCER_5_PATH} -file ${SATO_INDEX_FILE_NAME}  -mapper "${MAPPER_5} ${geomid} ${SATO_INDEX_FILE_NAME}" -reducer "${REDUCER_5} cat" -cmdenv LD_LIBRARY_PATH=${LD_CONFIG_PATH} -numReduceTasks ${numreducers}

if [  $? -ne 0 ]; then
   echo "Mapping data back to its partition has failed!"
fi

#hdfs dfs -rm ${prefixpath}/${SATO_INDEX_FILE_NAME}
hdfs dfs -cat ${prefixpath}/data/Stat/* | ../tiler/updatePartition.py ${SATO_INDEX_FILE_NAME} > ${PARTITION_FILE_DENORM}

hdfs dfs -put ${PARTITION_FILE_DENORM} ${prefixpath}/${SATO_INDEX_FILE_NAME}


rm -f ${SATO_INDEX_FILE_NAME}
rm -f ${PARTITION_FILE_DENORM}

hdfs dfs -rm -r -f ${prexipath}/data/Stat

echo "Data loaded into ${prefixpath}"
#TEMP_FILE_MERGE=/tmp/satomerge
# Merge small files together
#cat "${PARTITION_FILE}" | cut -f1 | { while read line
#do echo $line;
# hdfs dfs -getmerge ${prefixpath}/data/${line} ${TEMP_FILE_MERGE};
# hdfs dfs -rm -f -r  ${prefixpath}/data/${line};
# hdfs dfs -put ${TEMP_FILE_MERGE} ${prefixpath}/data/${line};
# rm ${TEMP_FILE_MERGE};
#done 
#}
