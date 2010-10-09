#!/bin/sh

aclocal -I m4
autoheader
libtoolize --copy --install
autoconf
automake --add-missing --copy --foreign
./configure "$@"
