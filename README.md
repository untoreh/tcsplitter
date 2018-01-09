## Tcsplitter

An end to end "tunnel" that demuxes a tcp connection into multiple connections each one having a predefined size. You could say each connection carries a datagram...eh!

Inbound data traffic is also initialized by the client for classic out-of-nat reach.

## Quickstart
Use scripts in `utils/`, put the binary in the same folder
```
## server side
./tcs-server.sh

## client side
./tcs-client.sh
```
start a local service on `127.0.0.1:6003`
connect the client on `127.0.0.1:6000`





## Flow
![flow](https://github.com/untoreh/tcsplitter/raw/master/flow.png)

## Dependencies 

- [urfave/cli](https://github.com/urfave/cli)
- [dscp]
- [...]