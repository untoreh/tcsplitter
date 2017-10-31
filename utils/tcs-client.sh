#!/bin/sh

# REMOTE_IP=212.237.6.194
REMOTE_IP=127.0.0.1
# proto=tcp
proto=udp

./tcsplitter_amd64 \
    --protocol $proto \
    --conns 5 \
    --payload 10000 \
    --rFling $REMOTE_IP:6091 \
    --rSync $REMOTE_IP:5998 \
    --lLasso 0 \
    --rLasso $REMOTE_IP:6899 \
    --lLassoR 0 \
    --rLassoR $REMOTE_IP:6989
