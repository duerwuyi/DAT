#!/bin/bash

if [ -f Makefile ]; then
    make distclean
fi
rm -rf autom4te.cache config.log config.status configure aclocal.m4 Makefile.in
autoreconf -if
mkdir -p build
cd build
../configure
make
cd ..
cp build/distranger .
