package main

import (
    "encoding/binary"
    "io"
    "log"
    "net"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/klauspost/reedsolomon"
    "github.com/urfave/cli"
)

// The Conn interface of choice
type Conn *net.TCPConn

// The Shot is each connection including the time point of execution, the payload and
// its (optional) its length
type Shot struct {
    client  []byte // int64 for TCP / 21bytes string (ipv4:port) for UDP
    ofs     []byte // uint32
    seq     []byte // uint32
    payload []byte
    ln      uint32
}

// Fec is struct for forward error correction options
type Fec struct {
    ds int // data shards
    ps int // parity shards
    ln int // data + parity
}

// Payload read from client or service
type Payload struct {
    data []byte
    ln   int
}

// Frags are fragment sizes
type Frags struct {
    payload int
    mtu     int
}

// ClientCmd shots to handle communication between tunnel ends
type ClientCmd struct {
    cmd    []byte // bool
    client []byte // int64 for TCP / 21bytes string (ipv4:port) for UDP
    data   []byte // depending on cmd
}

// Client holds the id of the client that is unix nano and its connection
type Client struct {
    end    byte   // 0 for client, 1 for server
    client []byte // int64 for TCP
    conn   interface{}
    seq    chan uint32 // current/next data seq
}

func main() {
    var prctl, frg, conns, lassoes, listen, lFling, rFling, forward,
        lLasso, rLasso, lFlingR, rFlingR, lSync, rSync, retries, to, ti, fec, dup string
    var rmtx, lmtx sync.Mutex
    // var rmtx sync.Mutex
    app := cli.NewApp()

    app.Flags = []cli.Flag{
        cli.StringFlag{
            Name:        "fec",
            Value:       "10:3",
            Usage:       "forward error correction flag",
            Destination: &fec,
        },
        cli.StringFlag{
            Name:        "dup",
            Value:       "both",
            Usage:       "enable duplex mode, possible values: fling,lasso,both,none (both)",
            Destination: &dup,
        },
        cli.StringFlag{
            Name:        "protocol",
            Value:       "tcp",
            Usage:       "protocol used by the client and server to be tunnelled over tcs (tcp)",
            Destination: &prctl,
        },
        cli.StringFlag{
            Name:        "frags",
            Value:       "4096:1308",
            Usage:       "size of the payload for each connection in bytes and mtu (4096:1308)",
            Destination: &frg,
        },
        cli.StringFlag{
            Name:        "tick",
            Value:       "33",
            Usage:       "unit of time to read a payload in milliseconds (33)",
            Destination: &ti,
        },
        cli.StringFlag{
            Name:        "tock",
            Value:       "333",
            Usage:       "timeout for merging small payloads in milliseconds (333)",
            Destination: &to,
        },
        cli.StringFlag{
            Name:        "conns",
            Value:       "5",
            Usage:       "the number of simultaneous connections for flinging (5)",
            Destination: &conns,
        },
        cli.StringFlag{
            Name:        "lassoes",
            Value:       "5",
            Usage:       "the number of simultaneous connections for lassoes (5)",
            Destination: &lassoes,
        },
        cli.StringFlag{
            Name:        "retries",
            Value:       "0" + conns,
            Usage:       "enable synced retries and tune skipping rate (0:conns)",
            Destination: &retries,
        },
        cli.StringFlag{
            Name:        "listen",
            Value:       "127.0.0.1:6000",
            Usage:       "address for clients connections to be tunneled (127.0.0.1:6000)",
            Destination: &listen,
        },
        cli.StringFlag{
            Name:        "lFling",
            Value:       "127.0.0.1:6090",
            Usage:       "local listening address for peers (127.0.0.1:6090)",
            Destination: &lFling,
        },
        cli.StringFlag{
            Name:        "rFling",
            Value:       "127.0.0.1:6091",
            Usage:       "address to send outgoing connections, a lFling of another peer (127.0.0.1:6091)",
            Destination: &rFling,
        },
        cli.StringFlag{
            Name:        "lLasso",
            Value:       "127.0.0.1:6899",
            Usage:       "address to listen to incoming lasso connections (127.0.0.1:6899)",
            Destination: &lLasso,
        },
        cli.StringFlag{
            Name:        "rLasso",
            Value:       "127.0.0.1:6900",
            Usage:       "remote address to send lassos to (127.0.0.1:6900)",
            Destination: &rLasso,
        },
        cli.StringFlag{
            Name:        "lFlingR",
            Value:       "127.0.0.1:6988",
            Usage:       "address to listen to incoming retry flings (127.0.0.1:6988)",
            Destination: &lFlingR,
        },
        cli.StringFlag{
            Name:        "rFlingR",
            Value:       "127.0.0.1:9600",
            Usage:       "remote address to send retry flings to (127.0.0.1:9600)",
            Destination: &rFlingR,
        },
        cli.StringFlag{
            Name:        "forward",
            Value:       "127.0.0.1:6003",
            Usage:       "address of the server to be tunneled (127.0.0.1:6003)",
            Destination: &forward,
        },
        cli.StringFlag{
            Name:        "lSync",
            Value:       "127.0.0.1:5999",
            Usage:       "address for listening to status syncronizations (127.0.0.1:5999)",
            Destination: &lSync,
        },
        cli.StringFlag{
            Name:        "rSync",
            Value:       "127.0.0.1:5998",
            Usage:       "remote peer address for status synchronizations (127.0.0.1:5998)",
            Destination: &rSync,
        },
    }

    app.Action = func(c *cli.Context) error {

        // flags conversions
        // connections
        conns := intString(conns)
        lassoes := intString(lassoes)
        // tick tock
        ti := intString(ti)
        tid := time.Duration(ti) * time.Millisecond
        to := intString(to)
        tod := time.Duration(to) * time.Millisecond
        // fec
        fecConf := Fec{}
        if fec != "0" {
            vals := coupleIntString(fec)
            fecConf = Fec{
                ds: vals[0],
                ps: vals[1],
                ln: vals[0] + vals[1],
            }
        }
        // frags
        frags := coupleIntString(frg)
        frg := Frags{
            payload: frags[0],
            mtu:     frags[1],
        }
        // retries
        retries := coupleIntString(retries)
        rtr := !(retries[0] == 0)
        skip := retries[1]

        // general errors channel
        errchan := make(chan error)
        // channel for flinging
        fchan := make(chan []byte, 2*conns)
        // channel for received flinged shots from remote
        rfchan := make(chan *Shot, 2*conns)
        // channel for ticker
        tichan := make(chan bool)
        // channel for tocker
        tochan := make(chan bool)

        // channel for shots to be caught by lassos
        pachan := make(chan *Shot, 2*lassoes)
        // channel for raw service->client shots (dup mode)
        padchan := make(chan []byte, 2*conns)
        // channel for shots caught from lassos
        cachan := make(chan *Shot, 2*lassoes)
        // channel for raw ClientCmd shots
        padRchan := make(chan []byte, 2)

        // holds direct client commands (like connection sync/updates)
        clcmdchan := make(chan *ClientCmd, 2*3)
        // holds reverse client commands
        crchan := make(chan *ClientCmd, 2*3)

        // conn queue channel for flings
        cq := make(chan Conn, conns)
        // new connections requests for conn queue channel (channel length to avoid write blocking)
        newcC := make(chan bool, 2*conns)
        // channel for issueing flushes of the retries channel
        schan := make(chan bool, 2*conns)

        // new connections queue for lassoes
        newcL := make(chan bool, 2*lassoes)
        // new connections queue for reverse lassoes
        newcR := make(chan bool, 2*3)

        switch prctl {
        case "tcp":
            // channels for local connections management
            addchan := make(chan *Client)
            rmchan := make(chan interface{})
            // holds clients connections buffered payloads for retries
            rtmap := make(map[int64]chan *ShotRtr)
            // holds clients connections ids
            clients := make(map[int64]bool)
            // holds clients connections payloads
            frmap := make(map[int64]map[uint32]*Shot)
            // holds lasso connections payloads
            flmap := make(map[int64]map[uint32]*Shot)
            // holds clients connections objects
            ccon := make(map[int64]Conn)
            // current offset map for forwarding remote/local
            rOfsMap := make(map[int64]uint32)
            lOfsMap := make(map[int64]uint32)

            go throttle(tichan, tid)
            go throttle(tochan, tod)
            go syncServer(errchan, &lSync, clients, clcmdchan)
            go clientServer(errchan, &listen, fchan, rtr, rtmap, schan, addchan, rmchan,
                frg, fecConf, conns, tichan, tochan, tid, tod)
            go syncHandler(addchan, rmchan, &rSync, clients, frmap, flmap, clcmdchan, ccon,
                &forward, pachan, padchan, padRchan, rtr, rtmap, &rmtx,
                frg, fecConf, tichan, tochan, tid, tod,
                conns, cq, newcC, schan, rOfsMap, lOfsMap)
            switch {
            case fecConf.ln != 0 && !rtr:
                go dispatchFec(rfchan, ccon, frg, fecConf, frmap, rOfsMap, &rmtx) // clientToServer dispatcher
                go dispatchFec(cachan, ccon, frg, fecConf, flmap, lOfsMap, &lmtx) // serverToClient dispatcher
            case fecConf.ln == 0 && !rtr:
                go dispatch(rfchan, ccon, frmap, rOfsMap, &rmtx, skip) // clientToServer dispatcher
                go dispatch(cachan, ccon, flmap, lOfsMap, &lmtx, skip) // serverToClient dispatcher
            case rtr:
                go dispatchRtr(rfchan, padRchan, ccon, frmap, &rmtx) // clientToServer dispatcher
                // the retry for reverse is not even implemented so channel is nil
                go dispatchRtr(cachan, nil, ccon, flmap, &lmtx) // serverToClient dispatcher
            }
        case "udp":
            // channels for local connections management
            addchan := make(chan *ClientUDP)
            rmchan := make(chan interface{})
            // holds clients connections buffered payloads for retries
            rtmap := make(map[string]chan *ShotRtr)
            // holds clients connections ids
            clients := make(map[string]*net.UDPAddr)
            // holds clients connections payloads
            frmap := make(map[string]map[uint32]*Shot)
            // holds lasso connections payloads
            flmap := make(map[string]map[uint32]*Shot)
            // holds clients connections objects, in udp only for server side
            ccon := make(map[string]*net.UDPConn)
            // current offset map for forwarding remote/local
            rOfsMap := make(map[string]uint32)
            lOfsMap := make(map[string]uint32)
            // channel for the single connection of the udp listener
            uchan := make(chan *net.UDPConn)

            go syncServerUDP(errchan, &lSync, clients, clcmdchan)
            go clientServerUDP(errchan, &listen, fchan,
                rtr, rtmap, schan,
                addchan, rmchan, clients, uchan,
                fecConf, conns, frg,
                tichan, tochan, tid, tod)
            go syncHandlerUDP(addchan, rmchan, &rSync, clients, frmap, flmap, clcmdchan, ccon,
                &forward, pachan, padchan, padRchan, rtr, rtmap, &rmtx,
                frg, fecConf, tichan, tochan, tid, tod,
                conns, cq, newcC, schan, rOfsMap, lOfsMap)
            if !rtr {
                go dispatchUDPServer(rfchan, crchan, ccon, frmap, rOfsMap, skip, &rmtx)              // clientToServer dispatcher
                go dispatchUDPClient(uchan, cachan, clcmdchan, clients, flmap, lOfsMap, skip, &lmtx) // serverToClient dispatcher
            } else {
                go dispatchUDPServerRtr(rfchan, padchan, ccon, frmap, &rmtx, skip)            // clientToServer dispatcher
                go dispatchUDPClientRtr(uchan, cachan, padRchan, clients, flmap, &lmtx, skip) // serverToClient dispatcher
            }

        }

        if rLasso != "0" {
            go lasso(&rLasso, cachan, fchan, frg, lassoes, newcL, tid, &dup, &prctl)

        }

        if lLasso != "0" {
            go lassoServer(errchan, &lLasso, padchan, rfchan, frg, tid, &dup)
        }

        if lFling != "0" {
            go flingServer(errchan, &lFling, rfchan, padchan, frg, conns, tid, &dup, &prctl)
        }

        if rFling != "0" {
            go fling(fchan, &rFling, cq, newcC, conns, cachan, frg, tid, &dup, &prctl)
        }
        if lFlingR != "0" {
            go lassoRServer(errchan, &lFlingR, padRchan)
        }
        if rFlingR != "0" {
            go lassoR(&rFlingR, clcmdchan, frg, lassoes, newcR, tid)
        }

        return <-errchan
    }

    e := app.Run(os.Args)
    log.Printf("tcs terminated, error: %v", e)
}

// listens to connections from client to be tunneled
func clientServer(errchan chan<- error, addr *string, fchan chan<- []byte,
    retries bool, rtmap map[int64]chan *ShotRtr, schan chan<- bool,
    addchan chan<- *Client, rmchan chan<- interface{}, frg Frags, fec Fec, conns int,
    tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) {
    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    ln, err := net.ListenTCP("tcp", addrTCP)
    if err != nil {
        errchan <- err
    }
    for {
        pcchan := make(chan Payload, 3*conns) // the payloads channel for each connection
        conn, err := ln.AcceptTCP()
        if err != nil {
            log.Println(err)
            continue
        }
        go tunnelPayloadsReader(pcchan, conn, frg, fec, tid, tod)
        go handleClientToTunnel(conn, fchan, retries, rtmap, addchan, rmchan,
            schan, pcchan, frg, tichan, tochan, tid, tod)
    }
}

// listens to connections from peers for data traffic
func flingServer(errchan chan<- error, addr *string, rfchan chan<- *Shot, padchan chan []byte,
    frg Frags, conns int, tid time.Duration, dup *string, prctl *string) {
    // the channel to avoid conn saturation
    fschan := make(chan bool, conns)
    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    ln, err := net.ListenTCP("tcp", addrTCP)
    if err != nil {
        errchan <- err
    }

    for {
        conn, err := ln.AcceptTCP()
        if err != nil {
            log.Println(err)
            continue
        }
        log.Printf("handling a received fling")
        go handleTunnelToTunnel(prctl, conn, rfchan, padchan, frg, conns,
            fschan, tid, defDup(dup))
    }

}

func defDup(dup *string) bool {
    switch *dup {
    case "both", "lasso":
        return true
    default:
        return false
    }
}

// listens to lasso connections from clients for reverse data traffic
func lassoServer(errchan chan<- error, addr *string, padchan chan []byte,
    rfchan chan *Shot, frg Frags, tid time.Duration, dup *string) {
    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    ln, err := net.ListenTCP("tcp", addrTCP)
    if err != nil {
        errchan <- err
    }

    var bdup bool
    switch *dup {
    case "both", "lasso":
        bdup = true
    default:
        bdup = false
    }

    for {
        conn, err := ln.AcceptTCP()
        if err != nil {
            log.Println(err)
            continue
        }
        // put the connection in the lassos channel
        go handleLasso(padchan, conn, rfchan, frg, tid, bdup)
    }

}

// listens to sync commands requests
func syncServer(errchan chan<- error, addr *string, clients map[int64]bool, cchan chan<- *ClientCmd) {
    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    ln, err := net.ListenTCP("tcp", addrTCP)
    if err != nil {
        errchan <- err
    }
    for {
        conn, err := ln.AcceptTCP()
        if err != nil {
            log.Println(err)
            continue
        }
        go handleSyncConnection(conn, clients, cchan)
    }
}

// queue up connections
func connQueue(addr *string, cq chan<- Conn, newc <-chan bool, st int) {

    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    cn := 0

    for cn < st {
        go func() {
            c, err := net.DialTCP("tcp", nil, addrTCP)
            if err != nil {
                log.Printf("failed creating connection for flinging: %v", err)
            }
            cq <- c

        }()
        cn++
    }
    for {
        <-newc
        go func() {
            c, err := net.DialTCP("tcp", nil, addrTCP)
            if err != nil {
                log.Printf("failed creating connection for flinging: %v", err)
            }
            cq <- c
        }()
    }
}

// sends shots to flingServer
func fling(fchan chan []byte, rFling *string, cq chan Conn, newc chan bool,
    st int, cachan chan<- *Shot, frg Frags, tid time.Duration, dup *string, prctl *string) {
    // set up connections pool
    go connQueue(rFling, cq, newc, st)

    // start flinging
    switch *dup {
    case "both", "fling":
        for c := range cq {
            // go shoot(fchan, c, newc, frg, true)
            go throw(prctl, cachan, c, nil, frg, tid, true) // only the shooter can spawn new flings

        }
    default:
        for c := range cq {
            go shoot(fchan, c, newc, frg, false)
        }

        // dynamic mode
        // newc <- true
        // for f := range fchan {
        // go shoot(f, fchan, <-cq, newc, frg, false)
        // newc <- true
        // }
    }
}

// write the fling to the connection and make sure it is received
// func shoot(fchan chan []byte, c Conn, newc chan<- bool, frg Frags, dup bool) {
func shoot(fchan chan []byte, c Conn, newc chan<- bool, frg Frags, dup bool) {

    // log.Printf("got a connection for flinging")
    dst := <-fchan
    // log.Printf("got a shot")
    // newc = nil

    //log.Printf("writing from %v to %v", t-mss, l)
    n, e := c.Write(dst)
    //n, e := c.Write(dst)
    //log.Printf("wrote %v bytes with the fling", n)
    if e != nil || n == 0 {
        // writing failed try again with another connection
        go qShot(dst, fchan)
        c.Close()
    } else {
        if dup {
            wClose(c, newc)
        } else {
            log.Printf("closing the fling conn")
            cClose(c, newc)
        }
        log.Printf("shot flung len: %v", len(dst))
        // putting connection in reading mode
    }
}

// close a connection, requeue a new one
func cClose(c Conn, newc chan<- bool) {
    go c.Close()
    if newc != nil {
        newc <- true
    }
}

// close read from tcp connection, requeue a new one
func rClose(c Conn, newc chan<- bool) {
    go (*c).CloseRead()
    if newc != nil {
        newc <- true
    }
}

// close read from tcp connection, requeue a new one
func wClose(c Conn, newc chan<- bool) {
    go (*c).CloseWrite()
    if newc != nil {
        newc <- true
    }
}

// Read from connection checking if closed and queueing up new ones
// successful read: true, true
// empty read: true, false
// io.EOF: false, true
// other error: false, false
func cRead(c Conn, pl []byte, n *int, newc chan<- bool) (bool, bool) {
    //log.Printf("doing a read")
    var e error
    *n, e = c.Read(pl)
    // log.Printf("did a read %v %v", n, e)
    switch {
    default:
        return true, true // we read and no errors were found
    case *n == 0 && (e == nil || e == io.EOF):
        rClose(c, newc)
        return false, false // we didnt read but error is EOF so w/e
    case *n != 0 && e != nil:
        rClose(c, newc)
        return true, false // we read something but there is an error so b/w
    case e != nil && e != io.EOF:
        rClose(c, newc)
        return false, false // the error is not EOF so false in any case
    }
}

// read from connection without queing up new conns on close
// generally want to close the connection on single reads
func cReadNoQ(c Conn, pl []byte, n *int, close bool) (bool, bool) {
    //log.Printf("doing a read")
    var e error
    *n, e = c.Read(pl)
    //log.Printf("did a read %v %v", *n, e)
    switch {
    default:
        return true, true // we read and no errors were found
    case *n == 0 && (e == nil || e == io.EOF):
        go (*c).CloseRead()
        return false, false // we didnt read but error is EOF so w/e
    case *n != 0 && e != nil && close:
        go (*c).CloseRead()
        return true, false // we read something but there is an error so b/w
    case *n != 0 && e != nil:
        return true, false // same as prev but don't close conn
    case e != nil && e != io.EOF && close:
        go (*c).CloseRead()
        return false, false // the error is not EOF so false in any case
    case e != nil && e != io.EOF:
        return false, false // same as prev but don't close conn
    }
}

// check if connesion is closed with a timeout
func isConnClosed(c Conn) bool {
    var b []byte
    var err error
    // log.Printf("doing a read")
    c.SetReadDeadline(time.Now().Add(1 * time.Second))
    _, err = c.Read(b)
    if e, ok := err.(net.Error); ok && e.Timeout() {
        return false // timeout
    } else if err == nil {
        return false
    }
    return true
}

// throw a bunch of connections for catching incoming data to the lassoServer
func lasso(rLasso *string, cachan chan<- *Shot, fchan chan []byte, frg Frags,
    lassoes int, newc chan bool, tid time.Duration, dup *string, prctl *string) {

    // a conn queue for lassoes
    cq := make(chan Conn, lassoes)

    go connQueue(rLasso, cq, newc, lassoes)
    // log.Printf("lasso connections should be starting now...")

    // a throw for each connection
    if defDup(dup) {
        for c := range cq {
            go throw(prctl, cachan, c, newc, frg, tid, true)
            go shoot(fchan, c, nil, frg, true) // only the throw can spawn new lassoes
        }
    } else {
        for c := range cq {
            go throw(prctl, cachan, c, newc, frg, tid, false)
        }
    }

}

// read shot fields queuing up a new connection on fail
func readShotFields(c Conn, reads [3][]byte, newc chan<- bool) bool {
    var n int
    for i, r := range reads {
        if _, b2 := cRead(c, r, &n, newc); !b2 {
            log.Printf("shot read error for field %v (Q)", i)
            return false
        }
    }
    return true
}

// read shot fields without queuing up new conns
func readShotFieldsNoQ(c Conn, reads [3][]byte) bool {
    var n int
    for i, r := range reads {
        if _, b2 := cReadNoQ(c, r, &n, true); !b2 {
            log.Printf("shot read error for field %v (NoQ)", i)
            return false
        }
    }
    return true
}

func readClientCmdFields(c Conn, reads [2][]byte, newc chan<- bool) bool {
    var n int
    for i, r := range reads {
        if _, b2 := cRead(c, r, &n, newc); !b2 {
            log.Printf("clientcmd read error for field %v", i)
            return false
        }
    }
    return true
}

// throw a single connection waiting for reading data
func throw(prctl *string, cachan chan<- *Shot, c Conn, newc chan<- bool, frg Frags, tid time.Duration, dup bool) {

    var n int
    shot := makeShot(*prctl, frg.payload)

    //if to != 0 {
    //log.Print("the deadline is %v", to)
    //c.SetReadDeadline(time.Now().Add(to))
    //}

    log.Printf("waiting to read shot fields (throw)")

    if !readShotFields(c, [3][]byte{shot.client, shot.ofs, shot.seq}, newc) {
        return
    }
    log.Printf("read shot fields (throw)")

    if !readShotPayload(c, shot.payload, frg, &n, newc, tid) {
        return
    }
    log.Printf("read payload (throw)")

    shot.ln = uint32(n)

    log.Printf("channeling lassoed shot, ofs: %v", intBytes(shot.ofs))
    cachan <- shot
    log.Printf("closing the connection")
    if dup {
        rClose(c, newc)
    } else {
        cClose(c, newc)
    }
}

func waitForMaps(ct int64, clientOfsMap map[int64]map[uint32]*Shot, wait map[int64]time.Duration) bool {
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
func dispatch(shotsChan <-chan *Shot, connMap map[int64]Conn,
    clientOfsMap map[int64]map[uint32]*Shot, cOfsMap map[int64]uint32, mtx *sync.Mutex, skip int) {
    cfmap := map[int64]int{}          // counter for consecutive failed forwarding attempts
    wait := map[int64]time.Duration{} // each client init waiting times
    for {
        // mtx.Lock()
        shot := <-shotsChan
        ct := timeBytes(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        if !waitForMaps(ct, clientOfsMap, wait) {
            continue // skip the shot
        }

        // only forward on time shots
        if ofs >= cOfsMap[ct] {
            clientOfsMap[ct][ofs] = shot
            // mtx.Lock()
            cOfsMap, cfmap[ct] = forward(ct, cOfsMap, cfmap[ct], clientOfsMap, connMap, skip)
            // mtx.Unlock()
        }

    }
}

func dispatchFec(shotsChan <-chan *Shot, connMap map[int64]Conn,
    frg Frags, fec Fec, clientOfsMap map[int64]map[uint32]*Shot, cOfsMap map[int64]uint32, mtx *sync.Mutex) {
    cfmap := map[int64]int{}          // counter for consecutive failed forwarding attempts
    wait := map[int64]time.Duration{} // each client init waiting times
    enc, _ := reedsolomon.New(fec.ds, fec.ps)
    // vars for nextAvl
    frgu := uint32(frg.payload)
    ps := uint32(fec.ps)
    for {
        // mtx.Lock()
        shot := <-shotsChan
        ct := timeBytes(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        if !waitForMaps(ct, clientOfsMap, wait) {
            log.Printf("skipping a shot")
            continue // skip the shot
        }

        // only forward on time shots
        if ofs >= cOfsMap[ct] {
            log.Printf("saving the shot")
            clientOfsMap[ct][ofs] = shot
            // mtx.Lock()
            cOfsMap, cfmap[ct] = fecForward(ct, cOfsMap, cfmap[ct], clientOfsMap, connMap, frgu, ps, &fec, enc)
            // mtx.Unlock()
        }

    }
}

// forward shots in an ordered manner to the right client
func forward(ct int64, cofs map[int64]uint32, cf int, clientOfsMap map[int64]map[uint32]*Shot,
    connMap map[int64]Conn, skip int) (map[int64]uint32, int) {
    for {
        // log.Printf("LOCAL forwarding...from tunneled server to client")
        // log.Printf("frmap seq keys for client are...\n")
        if shot, ready := clientOfsMap[ct][cofs[ct]]; ready {
            _, err := connMap[ct].Write(shot.payload[0:shot.ln])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            }
            // shot was written jump to the next
            log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
            delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
            cofs[ct] = intBytes(shot.seq)
            cf = 0 // reset failed forwarding

        } else if cf == skip { // skip the shot, sort the shots in the map and pick the lowest offset
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

// return the offset of the next availale shot for the given client's map
func nextAvl(ofsMap map[uint32]*Shot, ofs uint32, frg uint32, ps uint32) (uint32, bool) {
    var avl bool
    // the maximum offset for the first available shot else decoding fails
    // equals the payload size * parity shards
    maxofs := ofs + frg*ps
    for ofs <= maxofs {
        if _, avl = ofsMap[ofs]; avl {
            return ofs, true
        }
        log.Printf("nopee")
        ofs += frg
    }
    return 0, false
}

func fecForward(ct int64, cofs map[int64]uint32, cf int, clientOfsMap map[int64]map[uint32]*Shot,
    connMap map[int64]Conn, frg uint32, ps uint32, fec *Fec, enc reedsolomon.Encoder) (map[int64]uint32, int) {
    for {
        // if we have at least the length of a sharded whole payload in the shots map try to decode
        log.Printf("forward call %v, %v", len(clientOfsMap[ct]), cofs[ct])
        if len(clientOfsMap[ct]) >= fec.ds {
            log.Printf("trying to forward")
            // get the next ln shots from current offset
            shards := make([][]byte, fec.ln)
            ofs, ok := nextAvl(clientOfsMap[ct], cofs[ct], frg, ps) // the starting point of the next data set
            if !ok {
                break // try at next dispatch
            }
            ln := clientOfsMap[ct][ofs].ln // the next fec.ln payloads are gonna be this long
            lni := int(ln)
            ofs = cofs[ct] // go back to the current offset even if nil
            for i := range shards {
                if _, ok := clientOfsMap[ct][ofs]; ok { // missing shots are gonna be nil like the reconstructor wants
                    shards[i] = clientOfsMap[ct][ofs].payload
                }
                ofs += ln
            }
            if e := enc.ReconstructData(shards); e != nil {
                log.Printf("error reconstructing data, %v", e)
                return cofs, cf // exit the loop and wait for the next dispatch call
            }
            // join the dataset for writing
            data := make([]byte, int(ln)*fec.ds) // a slice big enough (or int(ln) * fec.ds)
            for i := range shards[:fec.ds] {
                // log.Printf("from: %v, to: %v", (i * lni), (i*lni + lni))
                copy(data[(i*lni):(i*lni+lni)], shards[i])
            }
            // write the whole payload
            pln := intBytes(data[:4]) + 4 // the declared payload length + offset
            log.Printf("length of data is %v, announced: %v", len(data), pln)
            _, err := connMap[ct].Write(data[4:pln])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            } else {
                log.Printf("forwarding successful, ofs: %v, seq: %v, ln: %v", cofs[ct], ofs, len(data))
                // clear the forwarded shot, loop again
                for i := range shards {
                    delete(clientOfsMap[ct], cofs[ct]+uint32(i*lni))
                }
                delete(clientOfsMap[ct], cofs[ct])
                cofs[ct] = ofs
                cf = 0 // reset failed forwarding
            }

        } else {
            cf++ // increase failed forwarding attempts
            break

        }
    }
    return cofs, cf
}

// channels payloads read from client or service
func tunnelPayloadsReader(cpchan chan<- Payload, c Conn, frg Frags, fec Fec,
    tid time.Duration, tod time.Duration) {

    var e error

    tichan := make(chan bool)
    tochan := make(chan bool)
    go throttle(tichan, tid)
    go throttle(tochan, tod)

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
        // size without the header
        hWpl := wpl - 4
        // var n, i, h, t int
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

            // read the whole data chunk, reduce data chunk by a heading uint32
            // for declaring the read length
            data := make([]byte, wpl)
            log.Printf("reading payload...from reader")
            n, e := readTunnelPayload(c, data[4:], hWpl, tichan, tochan, tid, tod)
            log.Printf("read payload...from reader")
            if e != nil || n == 0 {
                log.Printf("terminating tunnel to client/service connection: %v", e)
                break
            }
            // log.Printf("whole payload length: %v bytes ", n)
            copy(data[:4], bytesInt(uint32(n))) // prepend the length to the payload

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
                cpchan <- Payload{
                    data: shards[i],
                    ln:   frg.payload,
                }
            }
        }
    } else {
        for {
            payload := Payload{
                data: make([]byte, frg.payload),
            }
            log.Printf("reading tunnel payload")
            payload.ln, e = readTunnelPayload(c, payload.data, frg.payload, tichan, tochan, tid, tod)
            log.Printf("read tunnel payload %v", payload.ln)
            if e != nil && payload.ln == 0 {
                break
            } else {
                cpchan <- payload
            }
        }
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

// manage connections from clients to be tunneled
func handleClientToTunnel(c Conn, fchan chan<- []byte,
    retries bool, rtmap map[int64]chan *ShotRtr,
    addchan chan<- *Client, rmchan chan<- interface{},
    schan chan<- bool, pcchan <-chan Payload, frg Frags,
    tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) {

    // log.Printf("the bytes right now are : %v ", t)
    ct := time.Now().UnixNano()
    cl := Client{
        end:    0, // 0 for client 0
        client: bytesTime(ct),
        conn:   c,
        seq:    make(chan uint32, 1),
    }

    addchan <- &cl
    defer func() {
        rmchan <- &cl
    }()

    // log.Printf("shot client id is: %v", timeBytes(shot.client))
    seq := uint32(0)
    cl.seq <- seq
    for {
        payload, open := <-pcchan
        // this check is necessary otherwise the handler never stops when a client disconnects
        // leaving the connection data lingering
        if !open && payload.ln == 0 {
            log.Printf("terminating CTT handler")
            break
        }

        // the raw shot
        dst := make([]byte, (16 + payload.ln)) // make a slice big enough, 16 = 8 + 4 + 4
        // concatenate the shot fields
        copy(dst[0:], cl.client)     // client
        copy(dst[8:], bytesInt(seq)) // ofs

        var shotR ShotRtr
        if retries {
            // init retry shot here before we up the seq
            shotR = ShotRtr{
                ofs: seq,
            }
        }

        // continue concat
        // log.Printf("client->service ofs : %v, seq: %v", seq, seq+uint32(n))
        seq += uint32(payload.ln)
        copy(dst[12:], bytesInt(seq)) // seq
        // log.Printf("payload.data length: %v, payload.ln: %v", len(payload.data), payload.ln)
        copy(dst[16:], payload.data[0:payload.ln]) // payload

        // raw shots for the fling channel
        log.Printf("putting a dst into the fling channel, ofs: %v, len: %v", (seq - uint32(payload.ln)), payload.ln)
        fchan <- dst
        <-cl.seq
        cl.seq <- seq // shift the current seq

        if retries {
            // retry shots for the retry channel
            shotR.dst = dst
            go queueShotR(rtmap[ct], &shotR)
            schan <- true
        }
    }
}

// manages shots received from the remote end of the tunnel (through the fling server)
func handleTunnelToTunnel(prctl *string, c Conn, rfchan chan<- *Shot, padchan chan []byte, frg Frags,
    conns int, fschan chan bool, tid time.Duration, dup bool) {

    shot := makeShot(*prctl, frg.payload)
    if ok := readShot(shot, c, frg, tid, rfchan); !ok {
        return
    }

    writeDup(dup, c, conns, fschan, padchan)
    log.Printf("closed fling connection to %v", c.RemoteAddr())
}

func readShot(shot *Shot, c Conn, frg Frags, tid time.Duration, rfchan chan<- *Shot) bool {
    var n int
    log.Printf("reading shot fields from tunnel conn")
    if !readShotFieldsNoQ(c, [3][]byte{shot.client, shot.ofs, shot.seq}) {
        return false
    }
    log.Printf("read shot fields from tunnel conn")
    if !readShotPayloadNoQ(c, shot.payload, frg, &n, tid) {
        return false
    }
    log.Printf("read payload from tunnel conn")
    shot.ln = uint32(n)
    rfchan <- shot
    return true
}

func writeDup(dup bool, c Conn, conns int, fschan chan bool, padchan chan []byte) {
    if dup {
        go (*c).CloseRead()

        // don't saturate flinging connection for incoming data
        // log.Printf("fschan len %v, conns: %v", len(fschan), conns)
        if len(fschan) >= conns {
            log.Printf("closing lasso con before reads")
            (*c).Close()
            return
        }
        fschan <- true
        //log.Printf("fetching a raw shot for service->client, %v", c.RemoteAddr())
        dst := <-padchan

        //log.Printf("writing the fetched shot for server->client, %v", c.RemoteAddr())
        n, err := (*c).Write(dst)
        //log.Printf("wrote a raw shot for service->client, n: %v, len: %v, %v", n, len(dst), c.RemoteAddr())
        // put the shot back in the channel to be retried by another connection
        if n == 0 || err != nil {
            log.Printf("writing service->client shot to connection failed...")
            go qShot(dst, padchan)
        } else {
            log.Printf("wrote a shot of len %v to a fling", len(dst))
        }
        (*c).CloseWrite() // don't want a goroutine here
        <-fschan
    } else { // one way mode just close the connection
        c.Close()
    }
}

func qShot(shot []byte, ch chan<- []byte) {
    ch <- shot
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnection(c Conn, clients map[int64]bool, cchan chan<- *ClientCmd) {

    update := &ClientCmd{
        cmd:    make([]byte, 1),
        client: make([]byte, 8),
    }

    if ok := readSyncConnectionHeaders(c, update); !ok {
        return
    }
    if ok := readSyncConnectionData(c, update); !ok {
        return
    }

    endSyncConnection(c, update, cchan)
}

func readSyncConnectionHeaders(c Conn, update *ClientCmd) bool {
    // log.Printf("reading sync command")
    var n int
    if _, b2 := cReadNoQ(c, update.cmd, &n, false); !b2 {
        log.Printf("sync command read error")
        return false
    }

    // log.Printf("reading sync client")

    if _, b2 := cReadNoQ(c, update.client, &n, false); !b2 {
        log.Printf("sync client read error")
        return false
    }
    log.Printf("client sync connection is : %v", string(update.client))
    return true
}

func readSyncConnectionData(c Conn, update *ClientCmd) bool {
    var n int
    // log.Printf("reading sync data")
    switch {
    case update.cmd[0] == 2:
        // we say cmd 2 is for retries commands
        // so this needs to be an offset
        // so it is 4 bytes
        update.data = make([]byte, 4)
        if _, b2 := cReadNoQ(c, update.data, &n, true); !b2 {
            log.Printf("sync data read error")
            return false
        }
    case update.cmd[0] == 0:
        // cmd 0 is rm chan we need the seq to ensure all the shots are forwarded
        // before closing the connection, seq is 8 bytes like ofs
        update.data = make([]byte, 4)
        if _, b2 := cReadNoQ(c, update.data, &n, true); !b2 {
            log.Printf("sync data read error")
            return false
        }
    }
    return true
}

func endSyncConnection(c Conn, update *ClientCmd, cchan chan<- *ClientCmd) {
    cchan <- update
    log.Printf("sending ack")
    sndack(c, 100)
}

// sends the shots to waiting lasso connections
func handleLasso(padchan chan []byte, c Conn, rfchan chan *Shot, frg Frags, tid time.Duration, dup bool) {
    dst := <-padchan
    log.Printf("got a raw shot")
    n, e := c.Write(dst)
    if n == 0 || e != nil { // put the raw shot back into queue
        log.Printf("failed writing shot of len %v to lasso", len(dst))
        go qShot(dst, padchan)
    } else {
        log.Printf("wrote a shot of len %v to lasso", len(dst))
    }
    if dup { // if duplex mode close writing and start reading
        go (*c).CloseWrite()
        log.Printf("lasso writing was closed for %v", (*c).RemoteAddr())

        shot := makeShot("", frg.payload)
        log.Printf("reading shot fields from tunnel conn (lasso call)")
        if !readShotFieldsNoQ(c, [3][]byte{shot.client, shot.ofs, shot.seq}) {
            return
        }
        log.Printf("read shot fields from tunnel conn (lasso call)")
        if !readShotPayloadNoQ(c, shot.payload, frg, &n, tid) {
            return
        }
        log.Printf("read payload from tunnel conn (lasso call)")
        // log.Printf("received shot is: %v", pN+sN+tN+oN)
        shot.ln = uint32(n)
        rfchan <- shot

        // log.Printf("shot is read %v, putting in write mode", n)
        (*c).CloseRead()
        log.Printf("lasso reading was closed for %v", (*c).RemoteAddr())
    } else { // one way mode just close the connection
        c.Close()
    }
}

func makeShot(t string, s int) *Shot {
    switch {
    default:
        return &Shot{
            client:  make([]byte, 8),
            ofs:     make([]byte, 4),
            seq:     make([]byte, 4),
            payload: make([]byte, s),
        }
    case t == "udp":
        return &Shot{
            client:  make([]byte, 21),
            ofs:     make([]byte, 4),
            seq:     make([]byte, 4),
            payload: make([]byte, s),
        }
    }
}

// make raw for tcp mode
func makeRaw(shot *Shot) []byte {
    // make a slice big enough
    dst := make([]byte, (16 + shot.ln))
    // concatenate shot fieds
    copy(dst[0:], shot.client)
    copy(dst[8:], shot.ofs)
    copy(dst[12:], shot.seq)
    copy(dst[16:], shot.payload[0:shot.ln])
    log.Printf("shot with ofs: %v, dst len: %v", intBytes(shot.ofs), len(dst))
    return dst
}

// converts TCP tunneled shot to byte arrays
func rawMaker(t string, pachan <-chan *Shot, padchan chan<- []byte) {
    switch {
    default:
        for {
            padchan <- makeRaw(<-pachan)
        }
    case t == "udp":
        for {
            padchan <- makeRawUDP(<-pachan)
        }
    }
}

// reads the data from service to forward to client through lassoed connections
func serviceToTunnelHandler(prctl string, c interface{}, cl interface{}, rmchan chan<- interface{},
    pachan chan *Shot, padchan chan []byte,
    frg Frags, conns int, fec Fec,
    tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) {
    // the STT handler only needs the rmchan because of course a connection is only started by the client
    // so the server can only terminate it (abruptly)
    // cl := Client{
    // 	end:    1, // 1 for server end of the client instance
    // 	client: bytesTime(ct),
    // 	conn:   c,
    // 	seq:    make(chan uint32, 1),
    // }
    defer func() {
        rmchan <- cl
    }()

    // start the rawMaker
    go rawMaker(prctl, pachan, padchan)

    // start the payloads reader
    switch prctl {
    default:
        pcchan := make(chan Payload, 3*conns)
        go tunnelPayloadsReader(pcchan, c.(Conn), frg, fec, tid, tod)
        serviceShotsMaker("", cl.(*Client).seq, cl.(*Client).client, pachan, pcchan)
    case "udp":
        pcchan := make(chan *PayloadUDP, 3*conns)
        go tunnelPayloadsReaderUDP(pcchan, c.(*net.UDPConn), frg, fec, tichan, tochan, tid, tod)
        serviceShotsMaker("udp", cl.(*ClientUDP).seq, cl.(*ClientUDP).client, pachan, pcchan)
    }
}

// this function ends when the payloads reader channel is closed
func serviceShotsMaker(prctl string, seqchan chan uint32, ctB []byte, pachan chan *Shot, pcchan interface{}) {
    // process the shots
    seq := uint32(0)
    seqchan <- 0
    switch prctl {
    default:
        for {
            shot := Shot{
                client: ctB, // the client id is already decided remotely
            }
            //log.Printf("fetching a payload from service")
            payload, open := <-pcchan.(chan Payload)
            //log.Printf("fetched a payload from service")
            if !open {
                log.Printf("terminating STT handler")
                break
            }

            shot.payload = payload.data

            shot.ln = uint32(payload.ln)
            shot.ofs = bytesInt(seq)
            seq += shot.ln
            shot.seq = bytesInt(seq)
            // log.Printf("a shot for the pachan is on its way, ofs: %v", seq - uint32(n))
            pachan <- &shot
            <-seqchan
            seqchan <- seq // shift the current seq
        }
    case "udp":
        for {
            payload, open := <-pcchan.(chan *PayloadUDP)
            shot := Shot{
                client: ctB, // the client id is already decided remotely
            }
            //log.Printf("fetching a payload from service")
            //log.Printf("fetched a payload from service")
            if !open {
                log.Printf("terminating STT handler")
                break
            }

            shot.payload = payload.data

            shot.ln = uint32(payload.ln)
            shot.ofs = bytesInt(seq)
            seq += shot.ln
            shot.seq = bytesInt(seq)
            // log.Printf("a shot for the pachan is on its way, ofs: %v", seq - uint32(n))
            pachan <- &shot
            <-seqchan
            seqchan <- seq // shift the current seq
        }
    }

}

// keeps the clients list in sync accordingly to connections on the local listener and
// updates from remote peers on the status listener
func syncHandler(addchan <-chan *Client, rmchan chan interface{}, rSync *string, clients map[int64]bool,
    frmap map[int64]map[uint32]*Shot, flmap map[int64]map[uint32]*Shot, cchan <-chan *ClientCmd, ccon map[int64]Conn,
    forward *string, pachan chan *Shot, padchan chan []byte, padRchan chan []byte, retries bool, rtmap map[int64]chan *ShotRtr, mtx *sync.Mutex,
    frg Frags, fec Fec, tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration,
    conns int, cq <-chan Conn, newc chan<- bool, schan <-chan bool, rOfsMap map[int64]uint32, lOfsMap map[int64]uint32) {
    // rSync TCPAddr
    rSyncAddr, _ := net.ResolveTCPAddr("tcp", *rSync)
    // a mutex for the retry map
    rmtx := &sync.Mutex{}
    for {
        // loop over channels events to keep the list of persistent connections updated
        select {
        case client := <-addchan:
            ct := timeBytes(client.client)
            log.Printf("CT is %v", ct)
            mtx.Lock()
            clients[ct] = true
            ccon[ct] = client.conn.(Conn)
            frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
            flmap[ct] = make(map[uint32]*Shot) // init local payloads map
            if retries {
                qlen := conns * 100                   // the length of the buffered retry shots
                rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
                go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
            }
            log.Printf("payloads maps for %v initialized", ct)
            mtx.Unlock()

            update := ClientCmd{
                cmd:    []byte{1},
                client: client.client,
                // we don't need the seq on client add, maybe when adding a resume conn feature
            }
            sendClientUpdate(&update, rSyncAddr, nil, 100)
        case ifc := <-rmchan:
            client := ifc.(*Client)
            ct := timeBytes(client.client)
            mtx.Lock()
            if clients[ct] { // when the action is local, it is sure that the client is truely true or truely false
                seq := <-client.seq

                // this is the call on the local side, so the client is in the local lOfsMap
                go clearConn("", clients, ccon, frmap, flmap, rtmap, ct, 0, lOfsMap, mtx)

                update := &ClientCmd{
                    cmd:    []byte{0},
                    client: client.client,
                    data:   bytesInt(seq),
                }
                if client.end == 1 { // the server closed the connection so we need to use a reverse lasso
                    log.Printf("the server closed!")
                    sendClientUpdate(update, nil, padRchan, 100)
                } else { // the client closed so we dial to the server
                    log.Printf("the client closed!")
                    sendClientUpdate(update, rSyncAddr, nil, 100)
                }
            }
        case update := <-cchan: // this case is basically for server requests from remote clients
            ct := timeBytes(update.client)
            switch {
            case update.cmd[0] == 0: // this is the remove client command
                // this is the call on the remote side, so the client is in the remote rOfsMap
                // if clients[ct]
                // session already cleared
                // this happens because the connection clearing is ping pong
                // A gets closed ->
                // B gets notified and closes ->
                // B handler notices closed connection and re-sends the notification
                // B the connection is already cleared so return
                go clearConn("", clients, ccon, frmap, flmap, rtmap, ct, intBytes(update.data), rOfsMap, mtx)
                //}
            case update.cmd[0] == 1: // this is the add client command
                // initialize the client to the server
                mtx.Lock()
                if !addCt(ct, clients, ccon, rmchan,
                    frmap, flmap, rtmap,
                    pachan, padchan, schan,
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
                go refling(rtmap, update.client, update.data, cq, newc, rmtx)

            }
            // log.Printf("map updated!: \n")
            // spew.Dump(clients)
        }
    }
}

func whichSendClientUpdate(end byte, update *ClientCmd, rSyncAddr *net.TCPAddr, padRchan chan<- []byte) {
    if end == 1 { // the server closed the connection so we need to use a reverse lasso
        log.Printf("the server closed!")
        sendClientUpdate(update, nil, padRchan, 100)
    } else { // the client closed so we dial to the server
        log.Printf("the client closed!")
        sendClientUpdate(update, rSyncAddr, nil, 100)
    }
}

//updates the data for the requested ct
func addCt(ct int64, clients map[int64]bool, ccon map[int64]Conn, rmchan chan interface{},
    frmap map[int64]map[uint32]*Shot, flmap map[int64]map[uint32]*Shot, rtmap map[int64]chan *ShotRtr,
    pachan chan *Shot, padchan chan []byte, schan <-chan bool,
    tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration,
    conns int, retries bool, fec Fec, frg Frags, forward *string,
) bool {

    if clients[ct] {
        return true
    }

    // open local connection to reflect the synced clients list
    // the address is forward because we can only receive new connection
    // updates from clients asking for a connection on the service tunneled
    // through this peer.
    forwardAddr, err := net.ResolveTCPAddr("tcp", *forward)
    if err != nil {
        log.Printf("error resolving address %v: %v", forwardAddr, err)
        return false
    }
    if conn, err := net.DialTCP("tcp", nil, forwardAddr); err != nil {
        log.Printf("error syncying new connection %v: %v", forwardAddr, err)
        return false
    } else {
        ccon[ct] = conn
    }

    frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
    flmap[ct] = make(map[uint32]*Shot) // init local payloads map

    if retries {
        qlen := conns * 100                   // the length of the buffered retry shots
        rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
        go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
    }

    cl := &Client{
        end:    1, // 1 for server end of the client instance
        client: bytesTime(ct),
        conn:   ccon[ct],
        seq:    make(chan uint32, 1),
    }
    go serviceToTunnelHandler("", ccon[ct], cl, rmchan,
        pachan, padchan,
        frg, conns, fec,
        tichan, tochan, tid, tod)

    clients[ct] = true

    log.Printf("initialized client connection: %v", ct)
    return true
}

func sendClientUpdate(update *ClientCmd, rSyncAddr *net.TCPAddr, padRchan chan<- []byte, stuff int) {

    // different depending on the command
    var dst []byte
    // update.cmd is always len 1
    lenCl := len(update.client)
    lenData := len(update.data)
    switch {
    case update.cmd[0] == 1: // it is add chan
        // make a slice big enough 1 + 8
        dst = make([]byte, 1+lenCl)
        // concatenate shot fieds
        copy(dst[0:], update.cmd)
        copy(dst[1:1+lenCl], update.client)
    case update.cmd[0] == 0: // it is a rm chan
        // make a slice big enough 1 + 8 + 4
        dst = make([]byte, 1+lenCl+lenData)
        // concatenate shot fieds
        copy(dst[0:], update.cmd)
        copy(dst[1:1+lenCl], update.client)
        copy(dst[1+lenCl:(1+lenCl+lenData)], update.data) // data is seq number
    }
    if stuff != 0 { // stuff the payload if network drops small packets
        dst = append(dst, make([]byte, stuff)...)
    }
    // log.Printf("client update data len: %v", len(dst))
    var c Conn
    var err error
    if padRchan == nil { // if the channel is nil we dial the remote and write the command
        c, err = net.DialTCP("tcp", nil, rSyncAddr)
        if err != nil {
            log.Fatalf("error connecting to remote status server: %v", err)
        }
        //fill := make([]byte, 500)
        //dst := append(dst, fill...)
        (*c).Write(dst)
        (*c).CloseWrite()
        (*c).SetReadDeadline(time.Now().Add(5 * time.Second))
        log.Printf("waiting for ack")
        for !rcvack(c) { // retry until the update has been ack
            //log.Printf("retrying ack")
            c, err = net.DialTCP("tcp", nil, rSyncAddr)
            if err != nil {
                log.Fatalf("error connecting to remote status server: %v", err)
            }
            (*c).Write(dst)
            //time.Sleep(5*time.Second)
            (*c).CloseWrite()
            (*c).SetReadDeadline(time.Now().Add(15 * time.Second))
        }
        log.Printf("got ack")
    } else { // else we just queue the raw command to be managed by the lasso handler
        padRchan <- dst
    }

}

// send ack
func sndack(c Conn, pls int) bool {
    //log.Printf("sending ack")
    ret := 0
    dst := []byte{}
    if pls != 0 {
        dst = make([]byte, pls) // stuff the payload in case the network drops packets too small
        dst[0] = 1
    } else {
        dst = []byte{1}
    }
    for e := new(error); *e != nil && ret < 3; _, *e = c.Write(dst) {
        ret++
        //log.Printf("error sending ack: %v, retry %v", e, ret)
    }
    if ret > 0 {
        log.Printf("failed sending ack")
        (*c).CloseWrite()
        return false
    }
    //log.Printf("sent ack")
    (*c).CloseWrite()
    return true
}

// receive ack
func rcvack(c Conn) bool {
    ack := make([]byte, 1)
    n, err := c.Read(ack)
    if n != 8 && err != nil && err != io.EOF {
        log.Printf("ack failed n: %v, err: %v", n, err)
        (*c).CloseRead()
        return false
    }
    //log.Printf("ack succeded")
    (*c).CloseRead()
    return true
}

func clearConn(prctl string, clients interface{}, ccon interface{},
    frmap interface{}, flmap interface{}, rtmap interface{},
    ct interface{}, seq uint32, cOfsMap interface{}, mtx *sync.Mutex) {
    switch {
    default:
        // asserts
        ct := ct.(int64)
        clients := clients.(map[int64]bool)
        ccon := ccon.(map[int64]Conn)
        frmap := frmap.(map[int64]map[uint32]*Shot)
        flmap := flmap.(map[int64]map[uint32]*Shot)
        rtmap := rtmap.(map[int64]chan *ShotRtr)
        cOfsMap := cOfsMap.(map[int64]uint32)

        // get the last shot seq, wait until cseq is not 0 meaning it has started forwarding
        // in case of racing conditions between clients add and del
        // wait until the last shot has been forwarded
        log.Printf("clearing the conn for %v", ct)
        cSeq := cOfsMap[ct]
        for tries := 0; cSeq != seq && seq != 0 && clients[ct] && tries < 60; tries++ { // try 60 times ~ 1 minute
            log.Printf("waiting to delete the client: seq %v, cSeq %v", seq, cSeq)
            time.Sleep(1 * time.Second)
            cSeq = cOfsMap[ct]
        }
        delete(cOfsMap, ct)
        // remote client from client list
        delete(clients, ct)
        // close local connection to reflect the synced clients list
        if ccon[ct] != nil {
            ccon[ct].Close()
            delete(ccon, ct)
        }
        // delete payloads map
        delete(frmap, ct)
        delete(flmap, ct)
        // delete retries channel
        delete(rtmap, ct)
    case prctl == "udp":
        // asserts
        ct := ct.(string)
        clients := clients.(map[string]*net.UDPAddr)
        ccon := ccon.(map[string]*net.UDPConn)
        frmap := frmap.(map[string]map[uint32]*Shot)
        flmap := flmap.(map[string]map[uint32]*Shot)
        rtmap := rtmap.(map[string]chan *ShotRtr)
        cOfsMap := cOfsMap.(map[string]uint32)

        if clients[ct] == nil {
            return
        }
        // get the last shot seq
        cSeq := cOfsMap[ct]
        // wait until the last shot has been forwarded
        log.Printf("clearing the conn for %v", ct)
        for tries := 0; cSeq != seq && clients[ct] != nil && tries < 60; tries++ { // try 60 times ~ 1 minute
            log.Printf("waiting to delete the client: seq %v, cSeq %v", seq, cSeq)
            time.Sleep(1 * time.Second)
            cSeq = cOfsMap[ct]
        }
        delete(cOfsMap, ct)
        // remote client from client list
        delete(clients, ct)
        // close local connection to reflect the synced clients list
        if ccon[ct] != nil {
            ccon[ct].Close()
            delete(ccon, ct)
        }
        // delete payloads map
        delete(frmap, ct)
        delete(flmap, ct)
        // delete retries channel
        delete(rtmap, ct)

    }
    mtx.Unlock()
    log.Printf("connection %v cleared", ct)
}

// queue before sleeping to ensure only an action happens every interval
// queue after sleeping to ensure the span between each action is of interval

// timeout for cutting payload reads or
// time unit for every read
func throttle(tchan chan<- bool, td time.Duration) {
    for {
        tchan <- true
        time.Sleep(td)
    }
}

// handle the connection read timeout and empty reads
func ckRead(n int, err error) bool {
    if e, ok := err.(net.Error); ok && e.Timeout() {
        return true // timeout
    } else if n == 0 || err != nil {
        return false // empty payload or other error
    }
    return true
}

func isTimeout(err error) bool {
    if e, ok := err.(net.Error); ok && e.Timeout() {
        return true // timeout
    }
    return false
}

// read payload from client or from service following the timeout
func readTunnelPayload(c Conn, pl []byte, pll int,
    tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) (int, error) {
    var tn, n int
    var e error
    // first read
    // log.Printf("reading the first")
    c.SetReadDeadline(time.Time{}) // reset the deadline for the first read
    n, e = c.Read(pl[tn:])
    // log.Printf("read the first")
    if !ckRead(n, e) {
        // log.Printf("first read ck false..")
        return n, e
    }
    tn += n

    for tn < pll { // keep reading until payload slice is filled or a tock occurs
        select {
        case <-tichan:
            // tn is never 0 here
            c.SetReadDeadline(time.Now().Add(tod))
            // log.Printf("reading from client")
            n, e = c.Read(pl[tn:])
            // log.Printf("read from client %v", n)
            if !ckRead(n, e) {
                return tn, e
            }
            tn += n
        case <-tochan:
            // log.Printf("returning from tock")
            return tn, nil
        }
    }
    <-tochan // make sure we wait at least a tock
    return tn, nil
}

// read payload from lasso for dealing with mtu/mss
func readShotPayload(c Conn, pl []byte, frg Frags, tn *int, newc chan<- bool, tid time.Duration) bool {
    var n, tnv int

    // first read
    if _, b2 := cRead(c, pl[tnv:], &tnv, newc); !b2 {
        if tnv == 0 {
            // log.Printf("empty shot payload read...")
            return false
        }
    }

    // log.Printf("tnv: %v, frg: %v", tnv, frg)
    for tnv < frg.payload { // read until the slice is filled
        // log.Printf("conn: %v , payload length: %v", c.RemoteAddr().String(), len(pl))
        c.SetReadDeadline(time.Now().Add(5 * time.Second))
        if _, b2 := cRead(c, pl[tnv:], &n, newc); !b2 {
            // log.Printf("after read false")
            if tnv == 0 {
                return false
            }
            *tn = tnv
            return true
        }
        // log.Printf("after read true")
        tnv += n
    }
    *tn = tnv
    return true
}

// read shot payload without queuing up new connections
func readShotPayloadNoQ(c Conn, pl []byte, frg Frags, tn *int, tid time.Duration) bool {
    var n, tnv int

    // first read
    if _, b2 := cReadNoQ(c, pl[tnv:], &tnv, true); !b2 {
        if tnv == 0 {
            // log.Printf("empty shot payload read...")
            return false
        }
    }
    // log.Printf("after first read, tnv: %v, frg: %v", tnv, frg)
    // if tnv == payload covers the case mtu == pls
    // if tnv is multiple of mtu
    for tnv < frg.payload { // read until the slice is filled
        //log.Printf("tnv: %v maxRead: %v", tnv, maxRead)
        // log.Printf("setting read deadline to %v", tid)
        c.SetReadDeadline(time.Now().Add(5 * time.Second))
        if _, b2 := cReadNoQ(c, pl[tnv:], &n, true); !b2 {
            if tnv == 0 {
                // log.Printf("empty shot payload read...")
                return false
            }
            // log.Printf("returning true after second payload read NoQ")
            *tn = tnv
            return true
        }
        tnv += n
    }
    // log.Printf("wat")
    *tn = tnv
    return true
}

func bytesTime(t int64) []byte {
    if t == 0 {
        t = time.Now().UnixNano()
    }
    // log.Printf("Time now is: %v", t)
    bt := make([]byte, 8)
    binary.LittleEndian.PutUint64(bt, uint64(t))
    return bt
}

func timeBytes(ba []byte) int64 {
    if len(ba) == 0 {
        return 0
    }
    return int64(binary.LittleEndian.Uint64(ba))
}

func bytesInt(i uint32) []byte {
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, i)
    return bs
}

func intBytes(ba []byte) uint32 {
    return binary.LittleEndian.Uint32(ba)
}

func intString(str string) int {
    if i, e := strconv.Atoi(str); e != nil {
        log.Fatalf("can't convert parameter to string: %v", e)
    } else {
        return i
    }
    return 0
}

func boolString(str string) bool {
    if b, e := strconv.ParseBool(str); e != nil {
        log.Fatalf("can't convert parameter to string: %v", e)
    } else {
        return b
    }
    return false
}

func coupleIntString(str string) *[2]int {
    var e error
    couple := &[2]int{}
    ff := strings.Split(str, ":")
    couple[0], e = strconv.Atoi(ff[0])
    if e != nil {
        log.Fatalf("can't convert parameter to string: %v", e)
    }
    couple[1], e = strconv.Atoi(ff[1])
    if e != nil {
        log.Fatalf("can't convert parameter to string: %v", e)
    }
    return couple

}

// u32Slice is a slice of uint32
type u32Slice []uint32

func (a u32Slice) Len() int           { return len(a) }
func (a u32Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a u32Slice) Less(i, j int) bool { return a[i] < a[j] }
