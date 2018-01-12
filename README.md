## Tcsplitter

An end to end "tunnel" that demuxes a tcp connection into multiple connections each one having a predefined size. You could say each connection carries a datagram...eh!

Inbound data traffic is also initialized by the client for classic out-of-nat reach.

## Quickstart
Use scripts in `utils/`, put the binary `tcsplitter` in the same folder.
```
## server side
./tcs-server.sh

## client side
./tcs-client.sh
```
start a local service on `127.0.0.1:6003`
connect the client on `127.0.0.1:6000`

## Configuration
Important arguments are `tick` `tock` and `buffer`.

Tick determines how frequently one end of the tunnel is read.

Tock determines how frequently a payload is scheduled to be sent to the other end of the tunnel.

Buffer determines the frequency of forwarding data to the client or service once
it has been received through the tunnel, can be important to avoid chockings/hiccups and keep a stable data stream.

Note that to keep the tunnel interactive (i.e. ssh session) the `tock` of one end
should be the same of the `buffer` of the other end. This should not be a requirement
if the second paramater of buffer, that is the number of payloads to buffer, would merge payloads, so that 
it could be tweaked to be equal to the number of `connections` received, since each connection carries a payload.

There are a couple of config file to be used on linux (`sysctl -p sysctl.dirty`) to test different setups, dirty means to saturate connections, clean to use as little as possible.

## Other options
Most other options are not effectively functional at the moment.
### fec
As in `datashards:parityshards` like `10:3`, to be considered when using a lot of connections resulting in ack timeouts and whatnot.
### dup
Controls the direction of communication between tunnel ends.
- `fling` (default), connections from client to server are used both for incoming and outgoing data in an asynchronous manner, meaning that connections which are always running timeout if no payloads are provided and new ones are established.
- `lasso`, same as fling but lasso connections are used, this means the priority of the traffic is in favor of incoming data, *server -> client*
- `both`, full duplex, fling and lasso mode together, for high interaction for both outgoing and incoming data.
- `none`, simplex, connections only send data in one direction and are promptly closed afterwards.

### protocol
Tunnel TCP or UDP over TCP, currently only TCP effectively working.
### after
Warm up time in `ms` at the start of a connection, to complete handshakes and still being able to use high buffer times.
### frags
As in `payloadSize:MTU`, how much data each connection carries in `bytes`. MTU not currently used.
### conns/lassoes
How many fling connections and lasso connections to keep open at any given time.
### retries
As in `on/off:skippingRate`, the first value controls whether there should be attempts at retrasmitting lost payloads. This is best used in simplex mode (dup `none`) so that one end of the connection is used for acks. Otherwise the `sync` comm channel would be used to send retry requests. The `skippingRate` is instead best used with `fec` or when data integrity is not important in general as it simply skips over not available payloads after the specified number of attempts.
### listen
The target where data is sent to, i.e. client/service.
### lFling, rFling, lLasso, rLasso, lSync, rSync, lFlingR, rFlingR
Respectively the addresses in form `IPv4:PORT` of the local and remote endpoints for flings, lassoes, clients status/data synchronizations and retries.

## Stuff left to try, polishing, optimizations
See issues.

## Flow
Kind of outdated at this point.

![flow](https://github.com/untoreh/tcsplitter/raw/master/flow.png)

## Dependencies 

- [urfave/cli](https://github.com/urfave/cli)
- [dscp]
- [...]
