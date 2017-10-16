## Tcsplitter

An end to end "tunnel" that breaks a tcp connection into multiple connections each one having a predefined size. You could say each connection carries a datagram...eh!

Inbound data traffic is also initialized by the client for classic out-of-nat reach.

## Quickstart
```
## server side
tcsplitter --payload 10000 \
        --listen 127.0.0.1:6001 \
        --lFling 127.0.0.1:6091 \
        --lSync 127.0.0.1:5998 \
        --rLasso 0 \
        --lLasso 127.0.0.1:6899

## client side
 ./tcsplitter \
        --payload 10000 \
        --rFling 127.0.0.1:6091 \
        --rSync 127.0.0.1:5998 \
        --lLasso 0 \
        --rLasso 127.0.0.1:6899
```

## Flow


## Dependencies 

- [urfave/cli](https://github.com/urfave/cli)
- [fatih/pool.v2](gopkg.in/fatih/pool.v2)