#! /bin/bash


# Some example of installing on CentOS
yum install -y install cmake
yum install -y gcc-c++
# (or yum install –y gcc-4.7 g++-4.7)

wget http://download.osgeo.org/geos/geos-3.3.9.tar.bz2
tar xvf geos-3.3.9.tar.bz2
cd geos-3.3.9
mkdir Release
cd Release
cmake ..
make
sudo make install

cd
wget http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.1.tar.bz2
tar xvf spatialindex-src-1.8.1.tar.bz2
cd spatialindex-src-1.8.1
mkdir Release
cd Release
cmake ..
make
sudo make install

