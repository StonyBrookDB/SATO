
spatial=/home/aaji/softs

UNAME		= $(shell uname)
OPTFLAGS	= -O2 -Wno-write-strings
INCBASE		= -I. -I/usr/local/lib -I ${SATO_INC_PATH}
LIBBASE		= -lspatialindex -lgeos -L/usr/local/lib -L ${SATO_LIB_PATH}

ifneq (,$(findstring Linux,$(shell uname -a)))
    INCFLAGS = -I$(spatial)/include
    LIBS = -L$(spatial)/lib
else 
    ifeq ($(UNAME), Darwin)
    INCFLAGS = -I /Library/Frameworks/GEOS.framework/Headers
    LIBS = -L /Library/Frameworks/GEOS.framework/unix/lib
endif
endif

CFLAGS      = $(OPTFLAGS) $(INCBASE) $(INCFLAGS) 
LDFLAGS     = $(LIBBASE) $(LIBS)

ifeq ($(UNAME), Darwin)
  CC = g++ -arch x86_64
else 
  CC = g++
endif


all: hgtiler mbbextractor partitionMapper partitionMapperJoin partitionMapperSpec

debug: CXX += -DDEBUG -g
debug: CC += -DDEBUG -g
debug: hgtiler

# gengetopt -i options.ggo -F cmdline

cmd.o: options.ggo cmdline.h cmdline.c
	$(CC) -c cmdline.c -o cmd.o

hgtiler: cmd.o tiler.cpp hadoopgis.h tokenizer.h
	$(CC) tiler.cpp cmd.o $(CFLAGS) $(LDFLAGS) $(OPTFLAGS) -o hgtiler

mbbextractor: cmd.o mbbextractor.cpp hadoopgis.h tokenizer.h
	$(CC) mbbextractor.cpp cmd.o $(CFLAGS) $(LDFLAGS) $(OPTFLAGS) -o mbbextractor

partitionMapper: cmd.o partitionMapper.cpp hadoopgis.h tokenizer.h
	$(CC) -std=c++0x partitionMapper.cpp cmd.o $(CFLAGS) $(LDFLAGS) $(OPTFLAGS) -o partitionMapper

partitionMapperJoin: cmd.o partitionMapperJoin.cpp hadoopgis.h tokenizer.h
	$(CC) -std=c++0x partitionMapperJoin.cpp cmd.o $(CFLAGS) $(LDFLAGS) $(OPTFLAGS) -o partitionMapperJoin 

partitionMapperSpec: cmd.o partitionMapperSpec.cpp hadoopgis.h tokenizer.h
	$(CC) -std=c++0x partitionMapperSpec.cpp cmd.o $(CFLAGS) $(LDFLAGS) $(OPTFLAGS) -o partitionMapperSpec

clean:
	@rm -f *.o hgtiler mbbextractor

