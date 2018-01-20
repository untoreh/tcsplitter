#!/bin/sh

# listenip=10.10.12.209
listenip=localhost
pl=1292
mtu=1308
# pl=11756 # (+16)
# pl=8000
# pl=500
conns=12
bt=$((1000/conns))
to=$bt
ti=$((bt/2))
# proto=udp

# fling_port=80
fling_port=10900
# lasso_port=80
# rfling_port=20901
sync_port=30902
listen_port=6001

# forward="live-mil.twitch.tv:1935"
forward=127.0.0.1:22

exec ./tcsplitter \
     --frags $pl:$mtu \
     --conns $conns \
     --buffer $bt:0 \
     --tick $ti \
     --tock $to \
     --listen $listenip:$listen_port \
     --forward $forward \
     --lFling $listenip:$fling_port \
     --lSync $listenip:$sync_port
