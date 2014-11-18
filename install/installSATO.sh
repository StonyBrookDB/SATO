#! /bin/bash



# Configuring lib and include directories
usage(){
  echo -e "installSATO.sh [options]\n \
  -l PATH, --libpath=PATH \t directory path to the lib locations of dependencies (include lib)\n \
  -i PATH, --incpath=PATH \t directory path to the include locations"
  exit 1
}

# Default empty values
libpath=""
incpath=""

while : 
do
    case $1 in
        -h | --help | -\?)
          usage;
          exit 0
          ;;
        -l | --libpath)
          libpath=$2
          shift 2
          ;;
        --libpath=*)
          libpath=${1#*=}
          shift
          ;;
        -i | --incpath)
          incpath=$2
          shift 2
          ;;
        --incpath=*)
          incpath=${1#*=}
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

if [ ! "$incpath" ] && [ ! "$libpath"]; then
  echo "ERROR: Missing both options. See --help" >&2
  exit 1
fi
if [ ! "$incpath" ] ; then
  echo "ERROR: Missing include path. See --help" >&2
  exit 1
fi
if [ ! "$libpath" ] ; then
  echo "ERROR: Missing lib (library) path. See --help" >&2
  exit 1
fi

# Cd and running individual makefiles
echo $incpath
echo $libpath
SATO_INC_PATH=$incpath
SATO_LIB_PATH=$libpath

export SATO_INC_PATH=$incpath
export SATO_LIB_PATH=$libpath

echo "export SATO_INC_PATH=$incpath" > ~/.bashrc
echo "export SATO_LIB_PATH=$libpath" > ~/.bashrc

# Save the paths
echo "SATO_INC_PATH=${SATO_INC_PATH}" > ../sato.cfg
echo "SATO_LIB_PATH=${SATO_LIB_PATH}" >> ../sato.cfg

# cd to the directory where installSATO.sh is located.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
cd $SCRIPT_DIR

cd ../tiler
make

cd ../joiner
make

#cd ../step_tear/fg/serial
# make
cd ../step_tear/bsp/serial

make

#cd ../../rplus/serial
#make
#cd ../../rtree/serial
#make
#cd ../../sfc/serial
#make
#cd ../../strip/serial
#make

# Return to the install directory
cd ${SCRIPT_DIR} 

echo "Done with installation"
