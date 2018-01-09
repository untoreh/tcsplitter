#!/bin/sh

listenip=127.0.0.1
pl=10000
mtu=1308
#pl=11756 # (+16)
#pl=8000
#pl=500
#conns=100
conns=4
proto=udp
proto=tcp

#fling_port=8080
fling_port=20900
lasso_port=80
rfling_port=20901
sync_port=20902
listen_port=6001

#forward="live-mil.twitch.tv:1935"
forward=127.0.0.1:6003

exec ./tcsplitter \
--conns $conns \
--frags $pl:$mtu \
--listen $listenip:$listen_port \
--forward $forward \
--lFling $listenip:$fling_port \
--rFling 0 \
--lSync $listenip:$sync_port