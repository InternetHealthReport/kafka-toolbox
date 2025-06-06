#!/usr/bin/env bash

set -e

# detect operating system and distro name
source /etc/os-release
OS=$ID
DISTRO=$VERSION_CODENAME

case $OS in
    debian|ubuntu)
        CAIDA_ARCHIVE=https://pkg.caida.org/os/$OS
        ;;
    *)
        echo "unknown base system: $OS"
        exit 1
        ;;
esac

apt-get update
apt-get install curl apt-transport-https ssl-cert ca-certificates gnupg -y --no-install-recommends

# setup wandio repo
if [ ! -f /etc/apt/sources.list.d/wand-libwandio.list ]; then
    curl -1sLf 'https://dl.cloudsmith.io/public/wand/libwandio/cfg/setup/bash.deb.sh' | bash
fi

# setup libtrace repo
if [ ! -f /etc/apt/sources.list.d/wand-libtrace.list ]; then
    curl -1sLf 'https://dl.cloudsmith.io/public/wand/libtrace/cfg/setup/bash.deb.sh' | bash
fi

# setup libwandder repo
if [ ! -f /etc/apt/sources.list.d/wand-libwandder.list ]; then
    curl -1sLf 'https://dl.cloudsmith.io/public/wand/libwandder/cfg/setup/bash.deb.sh' | bash
fi

# setup CAIDA stable repo
if [ ! -f /etc/apt/sources.list.d/caida.list ]; then
    echo "deb $CAIDA_ARCHIVE $DISTRO main" | tee /etc/apt/sources.list.d/caida.list
    curl -so /etc/apt/trusted.gpg.d/caida.gpg $CAIDA_ARCHIVE/keyring.gpg
fi

apt-get update
apt-get install bgpstream python3-pybgpstream -y --no-install-recommends

rm -rf /var/lib/apt/lists/*