#!/bin/sh

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
    --lassoes 1 \
    --frags 10000:1308 \
    --rFling $REMOTE_IP:6091 \
    --rSync $REMOTE_IP:5998 \
    --rLasso $REMOTE_IP:6899 \
    --lLasso 0 \
    --lFlingR 0 \
    --rFlingR $REMOTE_IP:6989


# --rLasso 0 \
