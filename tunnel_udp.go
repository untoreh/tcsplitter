package main

import (
    "log"
    "net"
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/aristanetworks/goarista/dscp"
    "github.com/klauspost/reedsolomon"
)

type UDPListener struct {
    // Listener // inherits common options from listener
    // channels for local connections management
    addchan chan *ClientUDP
    rmchan  chan *ClientUDP
    // holds clients connections buffered payloads for retries
    rtmap map[string]chan *ShotRtr
    // holds clients connections ids
    clients map[string]*net.UDPAddr
    // holds clients connections payloads
    frmap map[string]map[uint32]*Shot
    // holds lasso connections payloads
    flmap map[string]map[uint32]*Shot
    // holds clients connections objects, in udp only for server side
    ccon map[string]*net.UDPConn
    // channel for the single connection of the udp listener
    uchan chan *net.UDPConn
}

// ClientUDP is the client structure for UDP tunneling
type ClientUDP struct {
    end    byte
    client []byte // 21bytes string (ipv4:port) for UDP
    conn   *net.UDPConn
    seq    chan uint32
}

// PayloadUDP with address field
type PayloadUDP struct {
    data []byte
    ln   int
    addr string
}

// ddlUDP for tracking timeouts of udp clients
type ddlUDP struct {
    clUDP *ClientUDP
    time  int
}

func clientServerUDP(errchan chan<- error, addr *string, fchan chan<- []byte,
    retries bool, rtmap map[string]chan *ShotRtr, schan chan<- bool,
    addchan chan<- *ClientUDP, rmchan chan<- interface{},
    clients map[string]*net.UDPAddr, uchan chan<- *net.UDPConn,
    fec Fec, conns int, frg *Frags,
    tichan [2]chan bool, tochan [2]chan bool, tid time.Duration, tod time.Duration) {

    udpaddr, _ := net.ResolveUDPAddr("udp", *addr)
    lp, err := net.ListenUDP("udp", udpaddr)
    if err != nil {
        log.Fatalf("error listening on %v: %v", *addr, err)
    }
    pcchan := make(chan *PayloadUDP, 3*conns)
    uchan <- lp
    go tunnelPayloadsReaderUDP(pcchan, lp, frg, fec, tichan, tochan, tid, tod)
    handleClientToTunnelUDP(lp, fchan, retries, rtmap, addchan, rmchan,
        schan, pcchan, frg,
        tichan, tochan, tid, tod,
        clients)
}

// listens to sync commands requests
func syncServerUDP(errchan chan<- error, addr *string, clients map[string]*net.UDPAddr, cchan chan<- *ClientCmd) {
    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    // ln, err := net.ListenTCP("tcp", addrTCP)
    tos := byte(46)
    ln, err := dscp.ListenTCPWithTOS(addrTCP, tos)
    if err != nil {
        errchan <- err
    }
    for {
        conn, err := ln.AcceptTCP()
        if err != nil {
            log.Println(err)
            continue
        }
        go handleSyncConnectionUDP(conn, clients, cchan)
    }
}

func reflingUDP(rtmap map[string]chan *ShotRtr, client []byte, data []byte, cq <-chan Conn, newc chan<- bool, rmtx *sync.Mutex) {
    ct := string(client)
    ofs := intBytes(data)
    // log.Printf("launching refling for offset: %v", ofs)
    rmtx.Lock()
    for shotR := range rtmap[ct] {
        if shotR.ofs != ofs {
            // log.Printf("skipping shot with ofs: %v", shotR.ofs)
            continue
        } else {
            // log.Printf("found the right shot to refling")
            shootRtr(shotR.dst, newc, cq)
            break
        }
    }
    rmtx.Unlock()
    // log.Printf("quitting refling")
}

func waitForMapsUDP(ct string, clientOfsMap map[string]map[uint32]*Shot, wait map[string]time.Duration) bool {
    for clientOfsMap[ct] == nil {
        if wait[ct] == 0 {
            wait[ct] = 10
        }
        if wait[ct] < 30000 {
            log.Printf("waiting for connection for client: %v", ct)
            time.Sleep(wait[ct] * time.Millisecond)
            wait[ct] = wait[ct] + 10
        } else {
            return false // skip the shot
        }
    }
    return true
}

// prepare received shots to be forwarded
// io bool true for client->tunnel (uses udp listener conn)
// io bool false for tunnel->service (uses dialed udp Conn)
func dispatchUDPServer(shotsChan <-chan *Shot, crchan chan<- *ClientCmd,
    connMap map[string]*net.UDPConn,
    clientOfsMap map[string]map[uint32]*Shot, cOfsMap map[string]uint32,
    skip int, mtx *sync.Mutex) {

    cfmap := map[string]int{}
    wait := map[string]time.Duration{} // each client init waiting times

    for {
        // mtx.Lock()
        shot := <-shotsChan
        ct := string(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        if !waitForMapsUDP(ct, clientOfsMap, wait) {
            continue // skip the shot
        }

        // only forward on time shots
        if ofs >= cOfsMap[ct] {
            clientOfsMap[ct][ofs] = shot
            cOfsMap, cfmap[ct] = forwardUDPServer(ct, cOfsMap, cfmap[ct], clientOfsMap, connMap, skip)
            // mtx.Unlock()
        }
    }
}

func dispatchUDPClient(uchan <-chan *net.UDPConn, shotsChan <-chan *Shot, crchan chan<- *ClientCmd,
    clients map[string]*net.UDPAddr, clientOfsMap map[string]map[uint32]*Shot, cOfsMap map[string]uint32,
    skip int, mtx *sync.Mutex) {

    cfmap := map[string]int{}          // counter for consecutive failed forwarding attempts
    wait := map[string]time.Duration{} // each client init waiting times
    c := <-uchan                       // wait for the connection of the UDP listener to be established

    for {
        // mtx.Lock()
        shot := <-shotsChan
        ct := string(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        if !waitForMapsUDP(ct, clientOfsMap, wait) {
            continue
        }

        // only forward on time shots
        if ofs >= cOfsMap[ct] {
            clientOfsMap[ct][ofs] = shot
            cOfsMap, cfmap[ct] = forwardUDPClient(c, ct, cOfsMap, cfmap[ct], clientOfsMap, clients, skip)
            // mtx.Unlock()
        }
    }
}

// forward shots in an ordered manner to the right client
func forwardUDPServer(ct string, cofs map[string]uint32, cf int, clientOfsMap map[string]map[uint32]*Shot,
    connMap map[string]*net.UDPConn, skip int) (map[string]uint32, int) {
    var err error
    for {
        // log.Printf("LOCAL forwarding...from tunneled server to client")
        ofs := cofs[ct]
        // log.Printf("frmap seq keys for client are...\n")
        if shot, ready := clientOfsMap[ct][ofs]; ready {
            _, err = connMap[ct].Write(shot.payload[0:shot.ln])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            }
            log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
            delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
            cofs[ct] = intBytes(shot.seq)
            cf = 0 // reset failed forwarding

        } else if cf == skip { // skip the shot
            // sort the shots in the map and pick the lowest offset
            o := 0
            offsets := make(u32Slice, len(clientOfsMap)) // if we want to use sort lib we need ints
            for ofs := range clientOfsMap[ct] {
                offsets[o] = ofs
            }
            sort.Sort(offsets)
            cofs[ct] = offsets[0] // this is the offset of the next shot
            cf = 0                // reset failed forwarding
            // write the shot
            shot = clientOfsMap[ct][cofs[ct]]
            _, err := connMap[ct].Write(shot.payload[0:shot.ln])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            }
            // shot was written jump to the next
            log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
            delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
            cofs[ct] = intBytes(shot.seq)
        } else {
            cf++  // increase failed forwarding attempts
            break // wait for next dispatch
        }
    }
    return cofs, cf
}

func forwardUDPClient(c *net.UDPConn, ct string, cofs map[string]uint32, cf int, clientOfsMap map[string]map[uint32]*Shot,
    clients map[string]*net.UDPAddr, skip int) (map[string]uint32, int) {
    var err error
    for {
        // log.Printf("LOCAL forwarding...from tunneled server to client")
        ofs := cofs[ct]
        // log.Printf("frmap seq keys for client are...\n")
        if shot, ready := clientOfsMap[ct][ofs]; ready {
            _, err = c.WriteToUDP(shot.payload[0:shot.ln], clients[ct])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            }
            log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
            delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
            cofs[ct] = intBytes(shot.seq)
            cf = 0 // reset failed forwarding
        } else if cf == skip {
            // sort the shots in the map and pick the lowest offset
            o := 0
            offsets := make(u32Slice, len(clientOfsMap)) // if we want to use sort lib we need ints
            for ofs := range clientOfsMap[ct] {
                offsets[o] = ofs
            }
            sort.Sort(offsets)
            cofs[ct] = offsets[0] // this is the offset of the next shot
            cf = 0                // reset failed forwarding
            // write the shot
            shot = clientOfsMap[ct][cofs[ct]]
            _, err := c.WriteToUDP(shot.payload[0:shot.ln], clients[ct])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            }
            // shot was written jump to the next
            log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
            delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
            cofs[ct] = intBytes(shot.seq)
        } else {
            cf++ // increase failed forwarding attempts
            break
        }
    }
    return cofs, cf
}

func handleClientToTunnelUDP(c *net.UDPConn, fchan chan<- []byte,
    retries bool, rtmap map[string]chan *ShotRtr,
    addchan chan<- *ClientUDP, rmchan chan<- interface{},
    schan chan<- bool, pcchan <-chan *PayloadUDP, frg *Frags,
    tichan [2]chan bool, tochan [2]chan bool, tid time.Duration, tod time.Duration,
    clients map[string]*net.UDPAddr) {
    seqmap := map[string]uint32{}
    var err error

    // log.Printf("shot client id is: %v", timeBytes(shot.client))

    cLock := &sync.Mutex{} // a lock for deadlining clients
    ddl := map[string]*ddlUDP{}
    go deadlineUDP(ddl, rmchan, cLock)

    for {
        // log.Printf("waiting for a paylaod")
        payload, open := <-pcchan
        // log.Printf("got a payload")
        if !open {
            log.Printf("terminating CTT handler (UDP)")
            break
        }
        // payload := make([]byte, frg.payload)
        // n, addr, err := c.ReadFrom(payload)
        // log.Printf("read %v bytes", n)
        // log.Printf("read payload is...\n%v", string(payload))
        // log.Printf("closing udp connection")
        // log.Printf("the shot to be sent has this payload (length %v)...%v", n, payload)
        ct := payload.addr
        lnCt := len(ct)
        if lnCt != 21 {
            ct = ct + strings.Repeat("\x00", 21-lnCt)
        }

        var clUDP *ClientUDP
        cLock.Lock()
        if clients[ct] == nil {
            // init ofs and seq
            seqmap[ct] = 0
            clients[ct], err = net.ResolveUDPAddr("udp", payload.addr)
            if err != nil {
                log.Printf("error resolving client udp address: %v", err)
            }

            clUDP = &ClientUDP{
                end:    0, // 0 for client
                client: []byte(ct),
                seq:    make(chan uint32, 1),
            }
            clUDP.seq <- 0

            addchan <- clUDP
            // deadline tracking
            ddl[ct] = &ddlUDP{
                clUDP: clUDP,
            }
        }
        // set client connection deadline
        ddl[ct].time = time.Now().Second() + 30
        cLock.Unlock()

        // the raw shot
        dst := make([]byte, (29 + payload.ln)) // make a slice big enough, 29 = 21 + 4 + 4
        // concatenate the shot fields
        copy(dst[0:], payload.addr)          // client
        copy(dst[21:], bytesInt(seqmap[ct])) // ofs
        // log.Printf("wrote ofs : %v", seqmap[cls])

        var shotR ShotRtr
        if retries {
            // init retry shot here before we up the seq
            shotR = ShotRtr{
                ofs: seqmap[ct],
            }
        }

        // continue concat
        seqmap[ct] += uint32(payload.ln)
        copy(dst[25:], bytesInt(seqmap[ct])) // seq
        // log.Printf("wrote seq: %v", seqmap[cls])
        copy(dst[29:], payload.data[0:payload.ln]) // payload

        // raw shots for the fling channel
        log.Printf("put a dst into the fling channel")
        fchan <- dst
        cLock.Lock()
        <-ddl[ct].clUDP.seq
        ddl[ct].clUDP.seq <- seqmap[ct]
        cLock.Unlock()

        if retries {
            // retry shots for the retry channel
            shotR.dst = dst
            go queueShotR(rtmap[payload.addr], &shotR)
            schan <- true
        }
    }
}

// delete udp clients not forwarding for more than 5 seconds
func deadlineUDP(ddl map[string]*ddlUDP, rmchan chan<- interface{}, cLock *sync.Mutex) {
    for {
        now := time.Now().Second()
        cLock.Lock()
        for _, ddlCl := range ddl {
            if ddlCl.time < now {
                rmchan <- ddlCl.clUDP
            }
        }
        cLock.Unlock()
        time.Sleep(5 * time.Second)
    }
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnectionUDP(c Conn, clients map[string]*net.UDPAddr, cchan chan<- *ClientCmd) {

    update := &ClientCmd{
        cmd:    make([]byte, 1),
        client: make([]byte, 21), // ipv4+port string = 21 chars
    }

    if ok := readSyncConnectionHeaders(c, update); !ok {
        return
    }
    if ok := readSyncConnectionData(c, update); !ok {
        return
    }

    endSyncConnection(c, update, cchan)
}

func makeRawUDP(shot *Shot) []byte {
    // make a slice big enough
    dst := make([]byte, (29 + shot.ln))
    // concatenate shot fieds
    copy(dst[0:], shot.client)
    copy(dst[21:], shot.ofs)
    copy(dst[25:], shot.seq)
    copy(dst[29:], shot.payload[0:shot.ln])
    log.Printf("shot with ofs: %v, dst len: %v", intBytes(shot.ofs), len(dst))
    return dst
}

func syncHandlerUDP(addchan <-chan *ClientUDP, rmchan chan interface{}, rSync *string, clients map[string]*net.UDPAddr,
    frmap map[string]map[uint32]*Shot, flmap map[string]map[uint32]*Shot, cchan <-chan *ClientCmd, ccon map[string]*net.UDPConn,
    forward *string, pachan chan *Shot, padchan chan []byte, padRchan chan []byte, retries bool, rtmap map[string]chan *ShotRtr, fwmap map[string][2]chan bool,
    mtx *sync.Mutex, frg *Frags, fec Fec, tichan [2]chan bool, tochan [2]chan bool, tid time.Duration, tod time.Duration,
    conns int, cq <-chan Conn, newc chan<- bool, schan <-chan bool, rOfsMap map[string]uint32, lOfsMap map[string]uint32) {
    // rSync TCPAddr
    rSyncAddr, _ := net.ResolveTCPAddr("tcp", *rSync)
    // a mutex for the retry map
    rmtx := &sync.Mutex{}
    for {
        // loop over channels events to keep the list of persistent connections updated
        select {
        case client := <-addchan:
            ct := string(client.client)
            mtx.Lock()
            // when creating the index key locally need to make sure it is 21 bytes long
            lnCt := len(ct)
            if lnCt != 21 {
                ct = ct + strings.Repeat("\x00", 21-lnCt)
            }
            qlen := conns * 100 // the length of the buffered retry shots

            // local writing for UDP is handled by the listener connection
            // log.Printf("local udp client to tunnel connection with ct %v, len %v", ct, len(ct))
            frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
            flmap[ct] = make(map[uint32]*Shot) // init local payloads map
            fwmap[ct] = makeClock()      // channel for throttling forwards
            go throttle(fwmap[ct], frg.bt)
            rOfsMap[ct] = 0                    // reset since in UDP client can reoccur
            lOfsMap[ct] = 0

            if retries {
                rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
                go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
            }
            log.Printf("payloads maps for %v initialized", ct)
            mtx.Unlock()

            update := ClientCmd{
                cmd:    []byte{1},
                client: client.client,
            }
            // only a client can add a connection
            sendClientUpdate(&update, rSyncAddr, nil, 100)
        case ifc := <-rmchan:
            log.Printf("removing a udp connection")
            client := ifc.(*ClientUDP)
            ct := string(client.client)
            mtx.Lock()
            if clients[ct] != nil {
                seq := <-client.seq

                go clearConn("udp", clients, ccon, frmap, flmap, rtmap, fwmap, ct, 0, lOfsMap, mtx)

                update := &ClientCmd{
                    cmd:    []byte{0},
                    client: client.client,
                    data:   bytesInt(seq),
                }
                // depending on who closed the connection choose the approprioate way to update
                whichSendClientUpdate(client.end, update, rSyncAddr, padRchan)
            }
        case update := <-cchan: // this case is basically for server requests from remote clients
            ct := string(update.client)

            switch {
            case update.cmd[0] == 0:
                mtx.Lock()
                go clearConn("udp", clients, ccon, frmap, flmap, rtmap, fwmap, ct, intBytes(update.data), rOfsMap, mtx)
            case update.cmd[0] == 1:
                mtx.Lock()
                if !addCtUDP(ct, clients, ccon, rmchan,
                    frmap, flmap, rtmap, fwmap,
                    pachan, padchan, schan, lOfsMap, rOfsMap,
                    tichan, tochan, tid, tod,
                    conns, retries, fec, frg, forward) {
                    // failed to connect notify client
                    update := &ClientCmd{
                        cmd:    []byte{0},
                        client: update.client,
                        data:   update.data,
                    }
                    sendClientUpdate(update, nil, padRchan, 100)
                }
                mtx.Unlock()
            case update.cmd[0] == 2: // this is a retry command
                // log.Printf("received a retry command")
                go reflingUDP(rtmap, update.client, update.data, cq, newc, rmtx)

            }
            // log.Printf("map updated!: \n")
            // spew.Dump(clients)
        }
    }
}

func addCtUDP(ct string, clients map[string]*net.UDPAddr, ccon map[string]*net.UDPConn, rmchan chan interface{},
    frmap map[string]map[uint32]*Shot, flmap map[string]map[uint32]*Shot, rtmap map[string]chan *ShotRtr, fwmap map[string][2]chan bool,
    pachan chan *Shot, padchan chan []byte, schan <-chan bool, lOfsMap map[string]uint32, rOfsMap map[string]uint32,
    tichan [2]chan bool, tochan [2]chan bool, tid time.Duration, tod time.Duration,
    conns int, retries bool, fec Fec, frg *Frags, forward *string) bool {
    if clients[ct] != nil { // already added
        return false
    }
    clients[ct], _ = net.ResolveUDPAddr("udp", strings.TrimSuffix(string(ct), "\x00"))
    udpForward, _ := net.ResolveUDPAddr("udp", *forward)

    // open local connection to reflect the synced clients list
    // the address is forward because we can only receive new connection
    // updates from clients asking for a connection on the service tunneled
    // through this peer.
    conn, err := net.DialUDP("udp", nil, udpForward)
    if err != nil {
        log.Printf("error dialing udp tunnel->service connection: %v", err)
        return false
    } else {
        ccon[ct] = conn
    }

    frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
    flmap[ct] = make(map[uint32]*Shot) // init local payloads map
    fwmap[ct] = makeClock()      // channel for throttling forwards
    go throttle(fwmap[ct], frg.bt)
    lOfsMap[ct] = 0                    // reset (in UDP the client can be repeated because it is based on the addres)
    rOfsMap[ct] = 0                    // reset

    if retries {
        qlen := conns * 100                   // the length of the buffered retry shots
        rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
        go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
    }

    cl := &ClientUDP{
        end:    1, // 1 for server end of the client instance
        client: []byte(ct),
        conn:   ccon[ct],
        seq:    make(chan uint32, 1),
    }
    go serviceToTunnelHandler("udp", ccon[ct], cl, rmchan,
        pachan, padchan, 
        frg, conns, fec,
        tichan, tochan, tid, tod)

    log.Printf("payloads maps for %v initialized", ct)
    // log.Printf("handled status case update, frmap : %v, flmap: %v", len(frmap), len(flmap))
    return true
}

func tunnelPayloadsReaderUDP(cpchan chan *PayloadUDP, c *net.UDPConn, frg *Frags, fec Fec,
    tichan [2]chan bool, tochan [2]chan bool, tid time.Duration, tod time.Duration) {

    var plchan chan *PayloadUDP

    if fec.ln != 0 {
        // get the encoder
        enc, e := reedsolomon.New(fec.ds, fec.ps)
        if e != nil {
            log.Printf("failed creating reedsolomon encoder: %v", e)
            close(cpchan)
            return
        }
        // whole payload to read is multiplied by the number of data shards
        wpl := frg.payload * fec.ds
        // start the payloads channeler, 4 is the header indicating the wpl size
        go readTunnelPayloadUDP(c, plchan, 4, wpl, tichan, tochan, tid, tod, false)
        var i int
        // generate the bounds for each data shard
        bounds := make([][2]int, fec.ds)
        for d := range bounds {
            bounds[d][0] = d * frg.payload
            bounds[d][1] = (d + 1) * frg.payload
        }
        for {
            shards := make([][]byte, fec.ln)
            i = 0

            // fetch the channeled data chunk already having offset for header
            log.Printf("reading payload...from reader")
            udpPl := <-plchan
            data := udpPl.data
            log.Printf("read payload...from reader")
            // log.Printf("whole payload length: %v bytes ", n)
            copy(data[:4], bytesInt(uint32(udpPl.ln))) // prepend the length to the payload

            // shards, e = enc.Split(data)
            // if e != nil {
            // 	log.Printf("error splitting the payload: %v", e)
            // }

            // shard the data
            for i = range shards[:fec.ds] {
                shards[i] = data[bounds[i][0]:bounds[i][1]]
            }
            // populate parity shards (using counter from data shards)
            for range shards[fec.ds:] {
                i++
                shards[i] = make([]byte, frg.payload)
            }
            // encode the shards
            e = enc.Encode(shards)
            if e != nil {
                log.Printf("error encoding data: %v", e)
            }

            // ok, e := enc.Verify(shards)
            // log.Printf("verify ok: %v, e: %v",ok, e)

            // channel each shard as a shot payload
            for i = range shards {
                cpchan <- &PayloadUDP{
                    data: shards[i],
                    ln:   frg.payload,
                    addr: udpPl.addr,
                }
            }
        }
    } else {
        // start directly the payloads channeler
        readTunnelPayloadUDP(c, cpchan, 0, frg.payload, tichan, tochan, tid, tod, false)
    }
    // closing connection and relative payloads channel
    c.Close()
    // wait for all the payloads to be processed
    for len(cpchan) > 0 {
        time.Sleep(1 * time.Second)
    }
    close(cpchan)
    log.Printf("stopped reading tunnel payloads")
}

// to be used with its own goroutine
func readTunnelPayloadUDP(c *net.UDPConn, plchan chan *PayloadUDP, head int, pll int,
    tichan [2]chan bool, tochan [2]chan bool, tid time.Duration, tod time.Duration, buf bool,
) {
    var n int
    var a string
    var na net.Addr
    var e error
    plmap := map[string]*PayloadUDP{}
    plmtx := &sync.Mutex{}
    // first read
    // log.Printf("reading the first")
    switch buf {
    case true:
        for {
            pl := make([]byte, pll) // slice to work with when reading

            c.SetReadDeadline(time.Now().Add(tod)) // wait max one tock
            // log.Printf("reading payload")
            n, na, e = c.ReadFrom(pl[head:])
            if na != nil {
                a = na.String()
            }
            bufferPayloadsUDP(n, a, e, pl, pll, plmap, plchan, plmtx, tochan, tod)
        }
    default:
        for {
            pl := make([]byte, pll) // slice to work with when reading

            c.SetReadDeadline(time.Now().Add(tod)) // wait max one tock
            // log.Printf("reading payload")
            n, na, e = c.ReadFrom(pl[head:])
            if na != nil {
                a = na.String()
            }
            if n != 0 {
                plchan <- &PayloadUDP{
                    data: pl,
                    ln:   n,
                    addr: a,
                }
            }
        }
    }

}

func bufferPayloadsUDP(n int, a string, e error, pl []byte, pll int,
    plmap map[string]*PayloadUDP, plchan chan *PayloadUDP, plmtx *sync.Mutex,
    tochan [2]chan bool, tod time.Duration) {
    if !ckRead(n, e) { // not a timeout, possibly EOF
        plmtx.Lock()
        switch {
        case plmap[a] != nil && n == 0: // flush existing payload
            plchan <- plmap[a]
            delete(plmap, a)
        case plmap[a] != nil && n != 0:
            switchPayloadUDP(plmap, plchan, n, pl, pll, a)
            if _, w := plmap[a]; w { // the switcher might have queued a remeaining payload
                plchan <- plmap[a]
                delete(plmap, a)
            }
        case plmap[a] == nil && n != 0: // channel it directly, probably the last packet
            plchan <- &PayloadUDP{
                data: pl,
                ln:   n,
                addr: a,
            }
        }
        plmtx.Unlock()
        return
    } else if n == 0 { // possibly a timeout with an empty read
        return
    } else { // the read was good
        // check if a previous payload carrying this address is waiting
        plmtx.Lock()
        if _, w := plmap[a]; w { // if a payload is waiting, copy some data
            switchPayloadUDP(plmap, plchan, n, pl, pll, a)
            plmtx.Unlock()
        } else { // no payloads waiting, make a new one
            pp := &PayloadUDP{
                data: pl,
                ln:   n,
                addr: a,
            }
            if n == pll { // it is already a full payload, channel
                plmtx.Unlock()
                <-tochan[0]
                plchan <- pp
            } else { // it is not full, map and deadline
                plmap[a] = pp
                go func() {
                    time.Sleep(tod)
                    plmtx.Lock()
                    if plmap[a] != nil {
                        plchan <- plmap[a]
                        delete(plmap, a)
                    }
                    plmtx.Unlock()
                }()
                plmtx.Unlock()
            }
        }
    }
    return
}

// decide what to do with a fresh udp payload
func switchPayloadUDP(plmap map[string]*PayloadUDP, plchan chan *PayloadUDP,
    n int, pl []byte, pll int, a string) {
    s := plmap[a].ln + n
    switch { // compare sizes
    case s > pll: // it is bigger: copy,channel,map the rest
        diff := pll - plmap[a].ln         // how much can we copy
        copy(plmap[a].data[plmap[a].ln:], // copy
            pl[:diff])
        plmap[a].ln = pll       // payload map is saturated
        plchan <- plmap[a]      // channel
        plmap[a] = &PayloadUDP{ // map the rest (a new payload)
            data: make([]byte, pll),
            ln:   n - diff,
            addr: a,
        }
        copy(plmap[a].data[0:], pl[diff:])
    case s == pll:
        copy(plmap[a].data[plmap[a].ln:], // copy all the new pl
            pl)
        plmap[a].ln = pll
        plchan <- plmap[a]
        delete(plmap, a) // clear the payload from the map
    case s < pll:
        copy(plmap[a].data[plmap[a].ln:], // copy all the new pl
            pl[:n])
        plmap[a].ln = s // the new size is the sum
    }
}
