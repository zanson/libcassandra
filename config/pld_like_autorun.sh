#!/bin/sh

libtoolize --copy --force --install
aclocal -I m4
autoconf
autoheader
automake -a -c -f --foreign


./configure LDFLAGS="-Wl,--as-needed -Wl,--no-copy-dt-needed-entries -Wl,-z,relro -Wl,-z,combreloc"  \
  CFLAGS="-O2 -fno-strict-aliasing -fwrapv -march=i686 -mtune=pentium4 -gdwarf-3 -g2"  \
  CXXFLAGS="-O2 -fno-strict-aliasing -fwrapv -march=i686 -mtune=pentium4 -gdwarf-3 -g2  -Wno-error"   \
  CPPFLAGS="-D_FORTIFY_SOURCE=2"   \
  CC=i686-pld-linux-gcc   \
  CXX=i686-pld-linux-g++  \
  --host=i686-pld-linux --build=i686-pld-linux --prefix=/usr --exec-prefix=/usr --bindir=/usr/bin --sbindir=/usr/sbin --sysconfdir=/etc  \
  --datadir=/usr/share --includedir=/usr/include --libdir=/usr/lib --libexecdir=/usr/lib --localstatedir=/var --sharedstatedir=/var/lib \
  --mandir=/usr/share/man --infodir=/usr/share/info --x-libraries=/usr/lib 

