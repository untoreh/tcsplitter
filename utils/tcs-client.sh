#!/bin/sh

#remoteip=148.251.3.246
remoteip=127.0.0.1
pl=10000
mtu=1308
#pl=8000
#pl=500
#conns=100
conns=4
#addr=192.168.42.129:6000
addr=127.0.0.1:6000
proto=udp
proto=tcp

## base
listen_port=6000
rfling_port=20900
sync_port=20902

exec ./tcsplitter \
        --listen $addr \
        --conns $conns \
        --frags $pl:$mtu \
        --rFling $remoteip:$rfling_port \
        --rSync $remoteip:$sync_port