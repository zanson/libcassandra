#! /bin/sh

# Tests install in ~/tmp/libcassandra_install_test
DESTDIR="${HOME}/tmp/libcassandra_install_test"

rm -r ${DESTDIR}
mkdir ${DESTDIR}

make install DESTDIR=${DESTDIR}


find ${DESTDIR}
