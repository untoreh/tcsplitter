#!/bin/sh

remoteip=88.198.69.215
pl=1292
mtu=1308
# pl=8000
# pl=500
conns=1
# conns=4
# skip=$((conns*4))
bt=$((1000/(conns)))
to=$bt
ti=$((bt/2))
lassoes=0
# addr=192.168.42.129:6000
addr=localhost
# proto=udp

## base
listen_port=6000
rfling_port=10900
# rfling_port=80
sync_port=10902

## extra
# fling_port=20900

exec ./tcsplitter \
     --frags $pl:$mtu \
     --conns $conns \
     --buffer $bt:0 \
     --tick $ti \
     --tock $to \
     --listen $addr \
     --rFling $remoteip:$rfling_port \
     --rSync $remoteip:$sync_port

