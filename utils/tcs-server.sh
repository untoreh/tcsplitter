#!/bin/sh -x

# REMOTE_IP=212.237.6.194
REMOTE_IP=127.0.0.1
# proto=tcp
proto=udp

./tcsplitter_amd64 \
    --dup none \
    --fec 0 \
    --retries 0:1000 \
    --tick 1 \
    --tock 1 \
    --protocol $proto \
    --conns 1 \
    --frags 10000:1308 \
    --listen $REMOTE_IP:6001 \
    --forward $REMOTE_IP:6003 \
    --lFling $REMOTE_IP:6091 \
    --rFling 0 \
    --lSync $REMOTE_IP:5998 \
    --lLasso 0 \
    --rLasso 0 \
    --rFlingR 0 \
    --lFlingR 0


    --lFlingR $REMOTE_IP:6989
--lLasso $REMOTE_IP:6899 \

