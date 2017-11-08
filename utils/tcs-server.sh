#!/bin/sh -x

# REMOTE_IP=212.237.6.194
REMOTE_IP=127.0.0.1
proto=tcp
# proto=udp

./tcsplitter_amd64 \
    --fec 10:3 \
    --tick 100 \
    --tock 200 \
    --protocol $proto \
    --conns 1 \
    --payload 10000 \
    --listen $REMOTE_IP:6001 \
    --lFling $REMOTE_IP:6091 \
    --rFling 0 \
    --lSync $REMOTE_IP:5998 \
    --lLasso $REMOTE_IP:6899 \
    --rLasso 0 \
    --rLassoR 0 \
    --lLassoR $REMOTE_IP:6989


--lLasso 0 \

