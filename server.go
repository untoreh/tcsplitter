package main

import (
    "encoding/binary"
    "io"
    "log"
    "net"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"

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
    ln      int
}
type ShotRtr struct {
    ofs uint32
    dst []byte // raw shot
}
type ClientCmd struct {
    cmd    []byte // bool
    client []byte // int64 for TCP / 21bytes string (ipv4:port) for UDP
    data   []byte // depending on cmd
}

type Client struct {
    client []byte // int64 for TCP
    conn   Conn
}

type ClientUDP struct {
    client []byte // 21bytes string (ipv4:port) for UDP
    conn   *net.UDPConn
}

func main() {
    var prctl, pls, conns, lassoes, listen, lFling, rFling, forward,
        lLasso, rLasso, lLassoR, rLassoR, lSync, rSync, retries, to, ti string
    var rmtx, lmtx sync.Mutex
    // var rmtx sync.Mutex

    app := cli.NewApp()

    app.Flags = []cli.Flag{
        cli.StringFlag{
            Name:        "protocol",
            Value:       "tcp",
            Usage:       "protocol used by the client and server to be tunnelled over tcs",
            Destination: &prctl,
        },
        cli.StringFlag{
            Name:        "payload",
            Value:       "4096",
            Usage:       "size of the payload for each connection in bytes",
            Destination: &pls,
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
            Usage:       "the number of simultaneous connections for flinging",
            Destination: &conns,
        },
        cli.StringFlag{
            Name:        "lassoes",
            Value:       "5",
            Usage:       "the number of simultaneous connections for lassoes",
            Destination: &lassoes,
        },
        cli.StringFlag{
            Name:        "retries",
            Value:       "0",
            Usage:       "enable retries for one-way mode (default false)",
            Destination: &retries,
        },
        cli.StringFlag{
            Name:        "listen",
            Value:       "127.0.0.1:6000",
            Usage:       "address for clients connections to be tunneled",
            Destination: &listen,
        },
        cli.StringFlag{
            Name:        "lFling",
            Value:       "127.0.0.1:6090",
            Usage:       "local listening address for peers",
            Destination: &lFling,
        },
        cli.StringFlag{
            Name:        "rFling",
            Value:       "127.0.0.1:6091",
            Usage:       "address to send outgoing connections (a lFlingof another peer)",
            Destination: &rFling,
        },
        cli.StringFlag{
            Name:        "lLasso",
            Value:       "127.0.0.1:6899",
            Usage:       "address to listen to incoming lasso connections",
            Destination: &lLasso,
        },
        cli.StringFlag{
            Name:        "rLasso",
            Value:       "127.0.0.1:6900",
            Usage:       "remote address to send lassos to",
            Destination: &rLasso,
        },
        cli.StringFlag{
            Name:        "lLassoR",
            Value:       "127.0.0.1:6988",
            Usage:       "address to listen to incoming reverse lasso connections",
            Destination: &lLassoR,
        },
        cli.StringFlag{
            Name:        "rLassoR",
            Value:       "127.0.0.1:9600",
            Usage:       "remote address to send reverse lassos to",
            Destination: &rLassoR,
        },
        cli.StringFlag{
            Name:        "forward",
            Value:       "127.0.0.1:6003",
            Usage:       "address of the server to be tunneled",
            Destination: &forward,
        },
        cli.StringFlag{
            Name:        "lSync",
            Value:       "127.0.0.1:5999",
            Usage:       "address for listening to status syncronizations",
            Destination: &lSync,
        },
        cli.StringFlag{
            Name:        "rSync",
            Value:       "127.0.0.1:5998",
            Usage:       "remote peer address for status synchronizations",
            Destination: &rSync,
        },
    }

    app.Action = func(c *cli.Context) error {

        // flags conversions
        pls, _ := strconv.Atoi(pls)
        conns, _ := strconv.Atoi(conns)
        lassoes, _ := strconv.Atoi(lassoes)
        retries, _ := strconv.ParseBool(retries)
        ti, _ := strconv.Atoi(ti)
        tid := time.Duration(ti)
        to, _ := strconv.Atoi(to)
        tod := time.Duration(to)

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

        // channel for lassos connections
        laschan := make(chan Conn, lassoes)
        // channel for reverse lassos connections
        larchan := make(chan Conn, 3)

        // channel for shots to be caught by lassos
        pachan := make(chan *Shot, 2*lassoes)
        // channel for raw service->client shots (dup mode)
        padchan := make(chan []byte, 2*conns)
        // channel for shots caught from lassos
        cachan := make(chan *Shot, 2*lassoes)

        // holds direct client commands (like connection sync/updates)
        cchan := make(chan *ClientCmd, 2*3)
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
            rmchan := make(chan *Client)
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

            go ticker(tichan, tid)
            go tocker(tochan, tod)
            go syncServer(errchan, &lSync, clients, cchan)
            go clientServer(errchan, &listen, fchan, retries, rtmap, schan, addchan, rmchan,
                pls, tichan, tochan, tid, tod)
            go syncHandler(addchan, rmchan, &rSync, clients, frmap, flmap, cchan, ccon,
                &forward, pachan, padchan, retries, rtmap, &rmtx,
                pls, tichan, tochan, tid, tod,
                conns, cq, newcC, schan)
            if !retries {
                go dispatch(rfchan, crchan, ccon, frmap, &rmtx) // clientToServer dispatcher
                go dispatch(cachan, cchan, ccon, flmap, &lmtx)  // serverToClient dispatcher
            } else {
                go dispatchRtr(rfchan, crchan, ccon, frmap, &rmtx) // clientToServer dispatcher
                go dispatchRtr(cachan, cchan, ccon, flmap, &lmtx)  // serverToClient dispatcher
            }
            if rLasso != "0" {
                go lasso(&rLasso, cachan, pls, lassoes, newcL)
            }
            if lLasso != "0" {
                go lassoServer(errchan, &lLasso, laschan)
                go toLassos(padchan, laschan)
            }
            if lFling != "0" {
                go flingServer(errchan, &lFling, rfchan, padchan, pls, conns)
            }
        case "udp":
            // channels for local connections management
            addchan := make(chan *ClientUDP)
            rmchan := make(chan *ClientUDP)
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
            // channel for the single connection of the udp listener
            uchan := make(chan *net.UDPConn)

            go syncServerUDP(errchan, &lSync, clients, cchan)
            go clientServerUDP(errchan, &listen, fchan, rtmap, addchan, rmchan, schan, clients, uchan, pls)
            go syncHandlerUDP(addchan, rmchan, &rSync, clients, frmap, flmap, cchan, ccon, &forward, pachan, retries, rtmap, &rmtx, pls, conns, cq, newcC, schan)
            if !retries {
                go dispatchUDPServer(rfchan, crchan, ccon, frmap, &rmtx)          // clientToServer dispatcher
                go dispatchUDPClient(uchan, cachan, cchan, clients, flmap, &lmtx) // serverToClient dispatcher
            } else {
                go dispatchUDPServerRtr(rfchan, crchan, ccon, frmap, &rmtx)          // clientToServer dispatcher
                go dispatchUDPClientRtr(uchan, cachan, cchan, clients, flmap, &lmtx) // serverToClient dispatcher
            }
            if rLasso != "0" {
                go lassoUDP(&rLasso, cachan, pls, lassoes, newcL)
            }
            if lLasso != "0" {
                go lassoServer(errchan, &lLasso, laschan)
                go toLassosUDP(pachan, laschan)
            }
            if lFling != "0" {
                go flingServerUDP(errchan, &lFling, rfchan, pls)
            }
        }

        if rFling != "0" {
            go fling(fchan, &rFling, cq, newcC, conns, cachan, pls)
        }
        if lLassoR != "0" {
            go lassoRServer(errchan, &lLassoR, larchan)
            go toLassosR(crchan, larchan)
        }
        if rLassoR != "0" {
            go lassoR(&rLassoR, cchan, pls, newcR)
        }

        return <-errchan
    }

    e := app.Run(os.Args)
    log.Printf("tcs terminated, error: %v", e)
}

// listens to connections from client to be tunneled
func clientServer(errchan chan<- error, addr *string, fchan chan<- []byte,
    retries bool, rtmap map[int64]chan *ShotRtr, schan chan<- bool,
    addchan chan<- *Client, rmchan chan<- *Client,
    pls int, tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) {
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
        go handleClientToTunnel(conn, fchan, retries, rtmap, addchan, rmchan, schan,
            pls, tichan, tochan, tid, tod)
    }
}

func clientServerUDP(errchan chan<- error, addr *string, fchan chan<- []byte,
    rtmap map[string]chan *ShotRtr, addchan chan<- *ClientUDP, rmchan chan<- *ClientUDP,
    schan chan<- bool, clients map[string]*net.UDPAddr, uchan chan<- *net.UDPConn, pls int) {
    udpaddr, _ := net.ResolveUDPAddr("udp", *addr)
    lp, err := net.ListenUDP("udp", udpaddr)
    if err != nil {
        log.Fatalf("error listening on %v: %v", *addr, err)
    }
    uchan <- lp
    handleClientToTunnelUDP(lp, fchan, rtmap, addchan, rmchan, schan, clients, pls)
}

// listens to connections from peers for data traffic
func flingServer(errchan chan<- error, addr *string, rfchan chan<- *Shot, padchan chan []byte, pls int, conns int) {
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
        // log.Printf("handling a received fling")
        go handleTunnelToTunnel(conn, rfchan, padchan, pls, conns, fschan)
    }

}

// listens to connections from peers for data traffic
func flingServerUDP(errchan chan<- error, addr *string, rfchan chan<- *Shot, pls int) {
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
        // log.Printf("handling a received fling")
        go handleTunnelToTunnelUDP(conn, rfchan, pls)
    }
}

// listens to lasso connections from clients for reverse data traffic
func lassoServer(errchan chan<- error, addr *string, lassos chan<- Conn) {
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
        // put the connection in the lassos channel
        go handleLassos(conn, lassos)
    }

}

// listens to reverse lasso connections from clients for reverse sync requests
func lassoRServer(errchan chan<- error, addr *string, lassosR chan<- Conn) {
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
        // put the connection in the lassos channel
        go handleLassosR(conn, lassosR)
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

// listens to sync commands requests
func syncServerUDP(errchan chan<- error, addr *string, clients map[string]*net.UDPAddr, cchan chan<- *ClientCmd) {
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
        go handleSyncConnectionUDP(conn, clients, cchan)
    }
}

// queue up connections
func connQueue(addr *string, cq chan<- Conn, newc <-chan bool, st int) {

    addrTCP, _ := net.ResolveTCPAddr("tcp", *addr)
    cn := 0

    for cn < st {
        c, err := net.DialTCP("tcp", nil, addrTCP)
        if err != nil {
            log.Printf("failed creating connection for flinging: %v", err)
        }
        cq <- c
        cn++
    }
    for {
        <-newc
        c, err := net.DialTCP("tcp", nil, addrTCP)
        if err != nil {
            log.Printf("failed creating connection for flinging: %v", err)
        }
        cq <- c
    }
}

// sends shots to flingServer
func fling(fchan chan []byte, rFling *string, cq chan Conn, newc chan bool, st int, cachan chan<- *Shot, pls int) {
    // set up connections pool
    go connQueue(rFling, cq, newc, st)

    // start flinging
    for {
        // if len(cq) > 1 { // make sure there is always at least a connection for flinging
        go shoot(<-fchan, fchan, newc, cq, cachan, pls)
        // log.Printf("shooting")
        // }
    }
}

// write the fling to the connection and make sure it is received
func shoot(dst []byte, fchan chan []byte, newc chan<- bool, cq <-chan Conn, cachan chan<- *Shot, pls int) {
    // log.Printf("waiting for a connection")
    c := <-cq
    // log.Printf("got a connection for flinging")
    // go rcvack(c, dst, newc, cq)
    n, e := c.Write(dst)
    if e != nil || n == 0 {
        // writing failed try again with another connection
        go qShot(dst, fchan)
        c.Close()
    } else {
        (*c).CloseWrite()
        log.Printf("shot flung len: %v", len(dst))
        // putting connection in reading mode
        // log.Printf("reading a shot from the flunged connection")
        go throw(c, newc, cachan, pls)
    }
}

// write the fling to the connection blindly
func shootRtr(dst []byte, newc chan<- bool, cq <-chan Conn) {
    c := <-cq
    c.Write(dst)
    c.Close()
    log.Printf("shot flung len: %v", len(dst))
    newc <- true
}

// skim throw the buffered shots and send the missing shot at offset
func refling(rtmap map[int64]chan *ShotRtr, client []byte, data []byte, cq <-chan Conn, newc chan<- bool) {
    ct := timeBytes(client)
    ofs := intBytes(data)
    // log.Printf("launching refling for offset: %v", ofs)
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
    // log.Printf("quitting refling")
}

func reflingUDP(rtmap map[string]chan *ShotRtr, client []byte, data []byte, cq <-chan Conn, newc chan<- bool) {
    ct := string(client)
    ofs := intBytes(data)
    // log.Printf("launching refling for offset: %v", ofs)
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
    // log.Printf("quitting refling")
}

// close a connection, requeue a new one
func cClose(c Conn, newc chan<- bool) {
    go c.Close()
    newc <- true
}

// close read from tcp connection, requeue a new one
func rClose(c Conn, newc chan<- bool) {
    go (*c).CloseRead()
    newc <- true
}

// close read from tcp connection, requeue a new one
func wClose(c Conn, newc chan<- bool) {
    go (*c).CloseWrite()
    newc <- true
}

// Read from connection checking if closed and queueing up new ones
func cRead(c Conn, pl []byte, n *int, newc chan<- bool) bool {
    // for n == 0 && e != io.EOF {
    // log.Printf("doing a read")
    // c.SetReadDeadline(time.Now().Add(1 * time.Second))
    var e error
    *n, e = c.Read(pl)
    // log.Printf("did a read %v %v", n, e)
    // }
    // log.Printf("returning from cRead with %v %v", n , e)
    if *n == 0 || e != nil {
        // log.Printf("error reading from %v, %v", c.RemoteAddr(), e)
        cClose(c, newc)
        return false
    }
    return true
}

// read from connection without queing up new conns on close
// generally want to close the connection on single reads
func cReadNoQ(c Conn, pl []byte, n *int, close bool) bool {
    // for n == 0 && e != io.EOF {
    // log.Printf("doing a read")
    // c.SetReadDeadline(time.Now().Add(1 * time.Second))
    var e error
    *n, e = c.Read(pl)
    // log.Printf("did a read %v %v", n, e)
    // }
    // log.Printf("returning from cRead with %v %v", n , e)
    if *n == 0 || e != nil {
        // log.Printf("error reading from %v, %v", c.RemoteAddr(), e)
        if close {
            go c.Close()
        }
        return false
    }
    return true
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
func lasso(rLasso *string, cachan chan<- *Shot, pls int, lassoes int, newc chan bool) {

    // a conn queue for lassoes
    cq := make(chan Conn, lassoes)

    go connQueue(rLasso, cq, newc, lassoes)
    // log.Printf("lasso connections should be starting now...")

    // a throw for each connection
    for c := range cq {
        go throw(c, newc, cachan, pls)
        // log.Printf("this is the %v lasso", t)
    }
}

// read shot fields queuing up a new connection on fail
func readShotFields(c Conn, reads [3][]byte, newc chan<- bool) bool {
    var n int
    for i, r := range reads {
        if !cRead(c, r, &n, newc) {
            log.Printf("shot read error for field %v", i)
            return false
        }
    }
    return true
}

// read shot fields without queuing up new conns
func readShotFieldsNoQ(c Conn, reads [3][]byte) bool {
    var n int
    for i, r := range reads {
        if !cReadNoQ(c, r, &n, true) {
            log.Printf("shot read error for field %v", i)
            return false
        }
    }
    return true
}

func readClientCmdFields(c Conn, reads [2][]byte, newc chan<- bool) bool {
    var n int
    for i, r := range reads {
        if !cRead(c, r, &n, newc) {
            log.Printf("clientcmd read error for field %v", i)
            return false
        }
    }
    return true
}

// throw a single connection waiting for reading data
func throw(c Conn, newc chan<- bool, cachan chan<- *Shot, pls int) {
    var n int
    shot := Shot{
        client:  make([]byte, 8),
        ofs:     make([]byte, 4),
        seq:     make([]byte, 4),
        payload: make([]byte, pls),
    }
    // log.Printf("waiting to read the client of the shot")

    if !readShotFields(c, [3][]byte{shot.client, shot.ofs, shot.seq}, newc) {
        return
    }

    // log.Printf("before reading payload...")
    if !readLassoPayload(c, shot.payload, &n, newc) {
        return
    }
    // log.Printf("read throw payload...")

    shot.ln = n

    log.Printf("channeling lassoed shot, ofs: %v", intBytes(shot.ofs))
    cachan <- &shot
    // cClose(c, newc)
    rClose(c, newc)
}

// throw a bunch of connections for catching incoming data to the lassoServer
func lassoUDP(rLasso *string, cachan chan<- *Shot, pls int, lassoes int, newc chan bool) {

    // a conn queue for lassoes
    cq := make(chan Conn, lassoes)

    go connQueue(rLasso, cq, newc, lassoes)

    // a throw for each connection
    for c := range cq {
        go throwUDP(c, newc, cachan, pls)
    }
}

// throw a single connection waiting for reading data
func throwUDP(c Conn, newc chan<- bool, cachan chan<- *Shot, pls int) {
    var n int
    shot := Shot{
        client:  make([]byte, 21),
        ofs:     make([]byte, 4),
        seq:     make([]byte, 4),
        payload: make([]byte, pls),
    }
    // log.Printf("starting reading who is the client from %v", c.LocalAddr().String())

    if !readShotFields(c, [3][]byte{shot.client, shot.ofs, shot.seq}, newc) {
        return
    }

    if !readLassoPayload(c, shot.payload, &n, newc) {
        return
    }

    shot.ln = n

    // log.Printf("channeling lassoed shot for udp")
    cachan <- &shot
    cClose(c, newc)
}

// lassos for server to client sync requests
func lassoR(rLassoRtr *string, cchan chan<- *ClientCmd, pls int, newc chan bool) {

    st := 3 // throws
    // a conn queue for lassoes
    cq := make(chan Conn, st)

    go connQueue(rLassoRtr, cq, newc, st)

    // a throw for each connection
    for c := range cq {
        go throwR(c, newc, cchan, pls)
    }
}
func throwR(c Conn, newc chan<- bool, cchan chan<- *ClientCmd, pls int) {
    var n int
    update := ClientCmd{
        cmd:    make([]byte, 1),
        client: make([]byte, 8),
        data:   make([]byte, pls),
    }
    // log.Printf("starting reading a reverse lasso")
    if !readClientCmdFields(c, [2][]byte{update.cmd, update.client}, newc) {
        return
    }
    if !readLassoPayload(c, update.data, &n, newc) {
        return
    }
    // the length of the data is known apriori because it is decided by the command number
    // log.Printf("channeling a reverse lasso")
    cchan <- &update
    cClose(c, newc)
}

// prepare received shots to be forwarded
func dispatch(shotsChan <-chan *Shot, crchan chan<- *ClientCmd, connMap map[int64]Conn, clientOfsMap map[int64]map[uint32]*Shot, mtx *sync.Mutex) {
    cofs := make(map[int64]uint32) // holds the current seq of each connection
    // rtMap := make(map[int64]bool)  // holds the retries
    cf := 0                        // counter for consecutive failed forwarding attempts
    for {
        // mtx.Lock()
        shot := <-shotsChan
        // log.Printf("client id is %v", shot.client)
        ct := timeBytes(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)
        st := time.Duration(10)
        for clientOfsMap[ct] == nil {
            log.Printf("waiting for connection to get established, clients count: %v", len(clientOfsMap))
            // log.Printf("clientOfsMap length: %v", len(clientOfsMap))
            time.Sleep(st * time.Millisecond)
            st = 2 * st
        }
        // schedule shots that actually are needed (in case a shot is received
        // while the same shot is received after a resync request)
        // if ofs >= cofs[ct] {
        clientOfsMap[ct][ofs] = shot
        // }
        // mtx.Lock()
        cofs, cf = forward(ct, cofs, cf, clientOfsMap, connMap)
        // mtx.Unlock()
    }
}

// prepare received shots to be forwarded
// io bool true for client->tunnel (uses udp listener conn)
// io bool false for tunnel->service (uses dialed udp Conn)
func dispatchUDPServer(shotsChan <-chan *Shot, crchan chan<- *ClientCmd,
    connMap map[string]*net.UDPConn,
    clientOfsMap map[string]map[uint32]*Shot, mtx *sync.Mutex) {
    cofs := make(map[string]uint32) // holds the current seq of each connection
    rtMap := make(map[string]bool)  // holds the retries
    cf := 0                         // counter for consecutive failed forwarding attempts
    for {
        // mtx.Lock()
        shot := <-shotsChan
        // log.Printf("client id is %v", shot.client)
        ct := string(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        // log.Printf("clientOfsMap is...")
        // spew.Dump(clientOfsMap)

        st := time.Duration(10)
        for clientOfsMap[ct] == nil {
            //log.Printf("waiting for connection to get established, clients count: %v", len(clientOfsMap))
            // log.Printf("clientOfsMap length: %v", len(clientOfsMap))
            st = 2 * st
            time.Sleep(st * time.Millisecond)
        }
        // schedule shots that actually are needed (in case a shot is received
        // while the same shot is received after a resync request)
        if ofs >= cofs[ct] {
            clientOfsMap[ct][ofs] = shot
        }
        // log.Printf("forwarding shots received from lassos...")
        // mtx.Lock()
        cofs, cf = forwardUDPServer(ct, cofs, cf, clientOfsMap, connMap)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            rtMap[ct] = retryUDP(ct, cofs[ct], crchan)
        }
        // mtx.Unlock()
    }
}

func dispatchUDPClient(uchan <-chan *net.UDPConn, shotsChan <-chan *Shot, crchan chan<- *ClientCmd,
    clients map[string]*net.UDPAddr, clientOfsMap map[string]map[uint32]*Shot, mtx *sync.Mutex) {
    cofs := make(map[string]uint32) // holds the current seq of each connection
    rtMap := make(map[string]bool)  // holds the retries
    cf := 0                         // counter for consecutive failed forwarding attempts
    c := <-uchan                    // wait for the connection of the UDP listener to be established
    for {
        // mtx.Lock()
        shot := <-shotsChan
        // log.Printf("got a caught shot")
        // log.Printf("client id is %v", shot.client)
        ct := string(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        // log.Printf("clientOfsMap is...")
        // spew.Dump(clientOfsMap)

        st := time.Duration(10)
        for clientOfsMap[ct] == nil {
            //log.Printf("waiting for connection to get established, clients count: %v", len(clientOfsMap))
            // log.Printf("clientOfsMap length: %v", len(clientOfsMap))
            st = 2 * st
            time.Sleep(st * time.Millisecond)
        }
        // schedule shots that actually are needed (in case a shot is received
        // while the same shot is received after a resync request)
        if ofs >= cofs[ct] {
            clientOfsMap[ct][ofs] = shot
        }
        // log.Printf("forwarding shots received from lassos...")
        // mtx.Lock()
        cofs, cf = forwardUDPClient(c, ct, cofs, cf, clientOfsMap, clients)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            log.Printf("retrying a shot...")
            rtMap[ct] = retryUDP(ct, cofs[ct], crchan)
        }
        // mtx.Unlock()
    }
}

// dispatch function with retry support
func dispatchRtr(shotsChan <-chan *Shot, crchan chan<- *ClientCmd, connMap map[int64]Conn, clientOfsMap map[int64]map[uint32]*Shot, mtx *sync.Mutex) {
    cofs := make(map[int64]uint32) // holds the current seq of each connection
    rtMap := make(map[int64]bool)  // holds the retries
    cf := 0                        // counter for consecutive failed forwarding attempts
    for {
        // mtx.Lock()
        shot := <-shotsChan
        // log.Printf("client id is %v", shot.client)
        ct := timeBytes(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)
        st := time.Duration(10)
        for clientOfsMap[ct] == nil {
            //log.Printf("waiting for connection to get established, clients count: %v", len(clientOfsMap))
            // log.Printf("clientOfsMap length: %v", len(clientOfsMap))
            time.Sleep(st * time.Millisecond)
            st = 2 * st
        }
        // schedule shots that actually are needed (in case a shot is received
        // while the same shot is received after a resync request)
        if ofs >= cofs[ct] {
            clientOfsMap[ct][ofs] = shot
        }
        // log.Printf("forwarding shots received from lassos...")
        // mtx.Lock()
        cofs, cf = forward(ct, cofs, cf, clientOfsMap, connMap)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            rtMap[ct] = retry(ct, cofs[ct], crchan)
        }
        // mtx.Unlock()
    }
}

// dispatchUDPClient function with retry support
func dispatchUDPClientRtr(uchan <-chan *net.UDPConn, shotsChan <-chan *Shot, crchan chan<- *ClientCmd,
    clients map[string]*net.UDPAddr, clientOfsMap map[string]map[uint32]*Shot, mtx *sync.Mutex) {
    cofs := make(map[string]uint32) // holds the current seq of each connection
    rtMap := make(map[string]bool)  // holds the retries
    cf := 0                         // counter for consecutive failed forwarding attempts
    c := <-uchan                    // wait for the connection of the UDP listener to be established
    for {
        // mtx.Lock()
        shot := <-shotsChan
        // log.Printf("got a caught shot")
        // log.Printf("client id is %v", shot.client)
        ct := string(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        // log.Printf("clientOfsMap is...")
        // spew.Dump(clientOfsMap)

        st := time.Duration(10)
        for clientOfsMap[ct] == nil {
            //log.Printf("waiting for connection to get established, clients count: %v", len(clientOfsMap))
            // log.Printf("clientOfsMap length: %v", len(clientOfsMap))
            st = 2 * st
            time.Sleep(st * time.Millisecond)
        }
        // schedule shots that actually are needed (in case a shot is received
        // while the same shot is received after a resync request)
        if ofs >= cofs[ct] {
            clientOfsMap[ct][ofs] = shot
        }
        // log.Printf("forwarding shots received from lassos...")
        // mtx.Lock()
        cofs, cf = forwardUDPClient(c, ct, cofs, cf, clientOfsMap, clients)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            log.Printf("retrying a shot...")
            rtMap[ct] = retryUDP(ct, cofs[ct], crchan)
        }
        // mtx.Unlock()
    }
}

// dispatchUDPServer function with retry support
func dispatchUDPServerRtr(shotsChan <-chan *Shot, crchan chan<- *ClientCmd,
    connMap map[string]*net.UDPConn,
    clientOfsMap map[string]map[uint32]*Shot, mtx *sync.Mutex) {
    cofs := make(map[string]uint32) // holds the current seq of each connection
    rtMap := make(map[string]bool)  // holds the retries
    cf := 0                         // counter for consecutive failed forwarding attempts
    for {
        // mtx.Lock()
        shot := <-shotsChan
        // log.Printf("client id is %v", shot.client)
        ct := string(shot.client)
        ofs := intBytes(shot.ofs)
        log.Printf("dispatching shot with ofs: %v", ofs)

        // log.Printf("clientOfsMap is...")
        // spew.Dump(clientOfsMap)

        st := time.Duration(10)
        for clientOfsMap[ct] == nil {
            //log.Printf("waiting for connection to get established, clients count: %v", len(clientOfsMap))
            // log.Printf("clientOfsMap length: %v", len(clientOfsMap))
            st = 2 * st
            time.Sleep(st * time.Millisecond)
        }
        // schedule shots that actually are needed (in case a shot is received
        // while the same shot is received after a resync request)
        if ofs >= cofs[ct] {
            clientOfsMap[ct][ofs] = shot
        }
        // log.Printf("forwarding shots received from lassos...")
        // mtx.Lock()
        cofs, cf = forwardUDPServer(ct, cofs, cf, clientOfsMap, connMap)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            rtMap[ct] = retryUDP(ct, cofs[ct], crchan)
        }
        // mtx.Unlock()
    }
}

// compose a client cmd request to be written to lassos
func retry(ct int64, ofs uint32, crchan chan<- *ClientCmd) bool {
    update := ClientCmd{
        cmd:    []byte{2}, // 2 is the chosen byte n for retries
        client: bytesTime(ct),
        data:   bytesInt(ofs),
    }
    // log.Printf("crafting a retry request")
    crchan <- &update
    return true
}

// compose a client cmd request to be written to lassos
func retryUDP(ct string, ofs uint32, crchan chan<- *ClientCmd) bool {
    update := ClientCmd{
        cmd:    []byte{2}, // 2 is the chosen byte n for retries
        client: []byte(ct),
        data:   bytesInt(ofs),
    }
    // log.Printf("crafting a retry request")
    crchan <- &update
    return true
}

// forward shots in an ordered manner to the right client
func forward(ct int64, cofs map[int64]uint32, cf int, clientOfsMap map[int64]map[uint32]*Shot, connMap map[int64]Conn) (map[int64]uint32, int) {
    // counter for forwarded packages
    for {
        // log.Printf("LOCAL forwarding...from tunneled server to client")
        ofs := cofs[ct]
        var err error
        // log.Printf("frmap seq keys for client are...\n")
        if shot, ready := clientOfsMap[ct][ofs]; ready {
            _, err = connMap[ct].Write(shot.payload[0:shot.ln])
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            } else {
                log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
                delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
                cofs[ct] = intBytes(shot.seq)
                cf = 0 // reset failed forwarding
            }
        } else {
            // log.Printf("shot not ready...")
            cf++ // increase failed forwarding attempts
            break
        }
    }
    return cofs, cf
}

// forward shots in an ordered manner to the right client
func forwardUDPServer(ct string, cofs map[string]uint32, cf int, clientOfsMap map[string]map[uint32]*Shot,
    connMap map[string]*net.UDPConn) (map[string]uint32, int) {
    // counter for forwarded packages
    for {
        // log.Printf("LOCAL forwarding...from tunneled server to client")
        ofs := cofs[ct]
        var err error
        // log.Printf("frmap seq keys for client are...\n")
        // log.Printf("checking ofsmap")
        if shot, ready := clientOfsMap[ct][ofs]; ready {
            // log.Printf("writiing in forward udp, using ct %v, len %v", ct, len(ct))
            // log.Printf("the connMap is this:")
            _, err = connMap[ct].Write(shot.payload[0:shot.ln])
            // log.Printf("did the writing")
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            } else {
                log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
                delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
                cofs[ct] = intBytes(shot.seq)
                cf = 0 // reset failed forwarding
            }
        } else {
            log.Printf("shot not ready...")
            cf++ // increase failed forwarding attempts
            break
        }
    }
    return cofs, cf
}

func forwardUDPClient(c *net.UDPConn, ct string, cofs map[string]uint32, cf int, clientOfsMap map[string]map[uint32]*Shot,
    clients map[string]*net.UDPAddr) (map[string]uint32, int) {
    // counter for forwarded packages
    for {
        // log.Printf("LOCAL forwarding...from tunneled server to client")
        ofs := cofs[ct]
        var err error
        // log.Printf("frmap seq keys for client are...\n")
        // log.Printf("checking ofsmap")
        if shot, ready := clientOfsMap[ct][ofs]; ready {
            // log.Printf("writiing in forward udp, using ct %v, len %v", ct, len(ct))
            // log.Printf("the UDP write to is this:")
            log.Printf("%v", clients[ct])
            _, err = c.WriteToUDP(shot.payload[0:shot.ln], clients[ct])
            // log.Printf("did the writing")
            if err != nil { // something wrong with the connection
                log.Printf("forward stopped: %v", err)
                return cofs, cf
            } else {
                log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
                delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
                cofs[ct] = intBytes(shot.seq)
                cf = 0 // reset failed forwarding
            }
        } else {
            log.Printf("shot not ready...")
            cf++ // increase failed forwarding attempts
            break
        }
    }
    return cofs, cf
}

// manage connections from clients to be tunneled
func handleClientToTunnel(c Conn, fchan chan<- []byte, retries bool, rtmap map[int64]chan *ShotRtr,
    addchan chan<- *Client, rmchan chan<- *Client, schan chan<- bool,
    pls int, tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) {

    // log.Printf("the bytes right now are : %v ", t)
    ct := time.Now().UnixNano()
    cl := Client{
        client: bytesTime(ct),
        conn:   c,
    }

    addchan <- &cl
    defer func() {
        rmchan <- &cl
    }()

    // log.Printf("shot client id is: %v", timeBytes(shot.client))
    seq := uint32(0)
    for {
        payload := make([]byte, pls)
        // log.Printf("reading a payload from client")
        n, err := readTunnelPayload(c, payload, tichan, tochan, tid, tod)
        // log.Printf("read a payload from client, %v", n)
        if err != nil && n == 0 {
            log.Printf("Closing local connection, error: %v", err)
            c.Close()
            break
        }
        // log.Printf("the shot to be sent has this payload (length %v)...%v", n, payload)

        // the raw shot
        dst := make([]byte, (16 + n)) // make a slice big enough, 16 = 8 + 4 + 4
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
        seq += uint32(n)
        copy(dst[12:], bytesInt(seq)) // seq
        copy(dst[16:], payload[0:n])  // payload

        // raw shots for the fling channel
        // log.Printf("putting a dst into the fling channel")
        fchan <- dst
        // log.Printf("put a dst into the fling channel")

        if retries {
            // retry shots for the retry channel
            shotR.dst = dst
            rtmap[ct] <- &shotR
            schan <- true
        }
    }
}

func handleClientToTunnelUDP(c *net.UDPConn, fchan chan<- []byte,
    rtmap map[string]chan *ShotRtr, addchan chan<- *ClientUDP, rmchan chan<- *ClientUDP,
    schan chan<- bool, clients map[string]*net.UDPAddr, pls int) {
    seqmap := map[string]uint32{}

    // log.Printf("shot client id is: %v", timeBytes(shot.client))
    for {
        payload := make([]byte, pls)
        n, addr, err := c.ReadFrom(payload)
        // log.Printf("read %v bytes", n)
        // log.Printf("read payload is...\n%v", string(payload))
        if err != nil {
            log.Printf("udp read connection, error: %v", err)
            c.Close()
            break
        }
        if n == 0 {
            // log.Printf("closing udp connection")
            cl := []byte(addr.String())
            rmchan <- &ClientUDP{client: cl}
            continue
        }
        // log.Printf("the shot to be sent has this payload (length %v)...%v", n, payload)
        ct := addr.String()
        lnCt := len(ct)
        if lnCt != 21 {
            ct = ct + strings.Repeat("\x00", 21-lnCt)
        }

        if clients[ct] == nil {
            // init ofs and seq
            seqmap[ct] = 0
            clients[ct], err = net.ResolveUDPAddr("udp", addr.String())

            if err != nil {
                log.Printf("error resolving client udp address: %v", err)
            }

            cl := []byte(ct)
            addchan <- &ClientUDP{client: cl}
            defer func() {
                time.Sleep(30 * time.Second)
                rmchan <- &ClientUDP{client: cl}
            }()
        }

        // the raw shot
        dst := make([]byte, (29 + n)) // make a slice big enough, 29 = 21 + 4 + 4
        // concatenate the shot fields
        copy(dst[0:], addr.String())         // client
        copy(dst[21:], bytesInt(seqmap[ct])) // ofs
        // log.Printf("wrote ofs : %v", seqmap[cls])

        // init retry shot here before we up the seq
        shotR := ShotRtr{
            ofs: seqmap[ct],
        }

        // continue concat
        seqmap[ct] += uint32(n)
        copy(dst[25:], bytesInt(seqmap[ct])) // seq
        // log.Printf("wrote seq: %v", seqmap[cls])
        copy(dst[29:], payload[0:n]) // payload

        // raw shots for the fling channel
        fchan <- dst
        // log.Printf("put a dst into the fling channel")

        // retry shots for the retry channel
        shotR.dst = dst
        go queueShotR(rtmap[addr.String()], &shotR)
        schan <- true
    }
}

// flush the channel for probably old unneeded shots retries
func rtFlusher(schan <-chan bool, rtc chan *ShotRtr, qlen int) {
    for {
        <-schan
        if len(rtc) == qlen {
            <-rtc
        }
    }
}

// queues the retry shot
func queueShotR(rtchan chan *ShotRtr, shotR *ShotRtr) {
    rtchan <- shotR
}

// manages shots received from the remote end of the tunnel (through the fling server)
func handleTunnelToTunnel(c Conn, rfchan chan<- *Shot, padchan chan []byte, pls int, conns int, fschan chan bool) {
    var n int

    shot := Shot{
        client:  make([]byte, 8),
        ofs:     make([]byte, 4),
        seq:     make([]byte, 4),
        payload: make([]byte, pls),
    }

    if !readShotFieldsNoQ(c, [3][]byte{shot.client, shot.ofs, shot.seq}) {
        return
    }

    if !readShotPayloadNoQ(c, shot.payload, &n) {
        return
    }

    // log.Printf("received shot is: %v", pN+sN+tN+oN)
    shot.ln = n
    rfchan <- &shot

    log.Printf("shot is read %v, putting in write mode", n)
    go (*c).CloseRead()
    log.Printf("closed read from %v", c.RemoteAddr())

    // don't saturate flinging connection for incoming data
    log.Printf("fschan len %v", len(fschan))
    if len(fschan) >= conns-1 {
        log.Printf("this connection has to close!")
        go c.Close()
        return
    }
    fschan <- true
    log.Printf("fetching a raw shot for service->client, %v", c.RemoteAddr())
    dst := <-padchan
    log.Printf("writing the fetched shot for server->client, %v", c.RemoteAddr())
    n, err := c.Write(dst)
    log.Printf("wrote a raw shot for service->client, n: %v, len: %v, %v", n, len(dst), c.RemoteAddr())
    // put the shot back in the channel to be retried by another connection
    if n == 0 || err != nil {
        log.Printf("writing service->client shot to connection failed...")
        go qShot(dst, padchan)
    }
    go wClose(c, fschan)
    log.Printf("closed connection to %v", c.RemoteAddr())
}

func qShot(shot []byte, ch chan<- []byte) {
    ch <- shot
}

// manages shots received from the remote end of the tunnel UDP
func handleTunnelToTunnelUDP(c Conn, rfchan chan<- *Shot, pls int) {
    var n int

    shot := Shot{
        client:  make([]byte, 21),
        ofs:     make([]byte, 4),
        seq:     make([]byte, 4),
        payload: make([]byte, pls),
    }

    if !readShotFieldsNoQ(c, [3][]byte{shot.client, shot.ofs, shot.seq}) {
        return
    }

    if !readShotPayloadNoQ(c, shot.payload, &n) {
        return
    }

    shot.ln = n

    // go sndack(c)
    rfchan <- &shot
    go c.Close()
    // log.Printf("channeled a shot")
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnection(c Conn, clients map[int64]bool, cchan chan<- *ClientCmd) {
    update := ClientCmd{
        cmd:    make([]byte, 1),
        client: make([]byte, 8),
    }
    _, err := c.Read([]byte(update.cmd))
    if err != nil {
        log.Printf("sync command read error: %v", err)
        c.Close()
        return
    }
    _, err = c.Read([]byte(update.client))
    if err != nil {
        log.Printf("sync client read error: %v", err)
        c.Close()
        return
    }
    if update.cmd[0] == 2 { // make this a switch eventually
        // we say cmd 2 is for retries commands
        // so this needs to be an offset
        // so it is 4 bytes
        update.data = make([]byte, 4)
        _, err = c.Read(update.data)
        if err != nil {
            log.Printf("sync data read error: %v", err)
            c.Close()
            return
        }
    }
    cchan <- &update
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnectionUDP(c Conn, clients map[string]*net.UDPAddr, cchan chan<- *ClientCmd) {
    update := ClientCmd{
        cmd:    make([]byte, 1),
        client: make([]byte, 21), // ipv4+port string = 21 chars
    }
    _, err := c.Read([]byte(update.cmd))
    if err != nil {
        log.Printf("sync command read error: %v", err)
        c.Close()
        return
    }
    _, err = c.Read([]byte(update.client))
    if err != nil {
        log.Printf("sync client read error: %v", err)
        c.Close()
        return
    }
    if update.cmd[0] == 2 { // make this a switch eventually
        // we say cmd 2 is for retries commands
        // so this needs to be an offset
        // so it is 4 bytes
        update.data = make([]byte, 4)
        _, err = c.Read(update.data)
        if err != nil {
            log.Printf("sync data read error: %v", err)
            c.Close()
            return
        }
    }
    cchan <- &update
}

// put lassos in the lassos channel
func handleLassos(c Conn, lassos chan<- Conn) {
    lassos <- c
    return
}

// put reverse lassos in the reverse lassos channel
func handleLassosR(c Conn, lassosR chan<- Conn) {
    lassosR <- c
    return
}

// sends the shots to waiting lasso connections
func toLassos(padchan chan []byte, laschan <-chan Conn) {
    for {
        dst := <-padchan
        // log.Printf("got a shot for a lasso")
        lasso := <-laschan // get a lasso
        // log.Printf("got a lasso")
        n, e := lasso.Write(dst)
        if n == 0 || e != nil { // put the raw shot back into queue
            go qShot(dst, padchan)
        }
        log.Printf("wrote a shot of len %v to lasso", len(dst))
        lasso.Close()
        log.Printf("lasso was closed")
        // log.Printf("closed lasso with error: %v", e)
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
func rawMaker(pachan <-chan *Shot, padchan chan<- []byte) {
    for {
        padchan <- makeRaw(<-pachan)
    }
}

func toLassosUDP(pachan <-chan *Shot, laschan <-chan Conn) {
    for {
        shot := <-pachan
        lasso := <-laschan // get a lasso
        // make a slice big enough
        dst := make([]byte, (29 + shot.ln))
        // concatenate shot fieds
        copy(dst[0:], shot.client)
        copy(dst[21:], shot.ofs)
        copy(dst[25:], shot.seq)
        copy(dst[29:], shot.payload[0:shot.ln])
        lasso.Write(dst)
        log.Printf("wrote a shot to a lasso ofs: %v, seq:: %v", intBytes(shot.ofs), intBytes(shot.seq))
        e := lasso.Close()
        log.Printf("closed lasso with error: %v", e)
    }
}

// sends client commands to waiting reverse lasso connections
func toLassosR(crchan chan *ClientCmd, larchan <-chan Conn) {
    for {
        update := <-crchan
        // log.Printf("got a retry request")
        lassoR := <-larchan // get a reverse lasso
        // log.Printf("got a retry lasso")
        // make a slice big enough
        dst := make([]byte, (9 + len(update.data))) // 1 + 8 + data length
        // concatenate shot fieds
        copy(dst[0:], update.cmd)
        copy(dst[1:], update.client)
        copy(dst[9:], update.data)
        n, e := lassoR.Write(dst)
        if n == 0 || e != nil {
            go qUpdate(update, crchan)
        }
        log.Printf("reverse lasso: wrote a command %v, ofs %v", update.cmd, intBytes(update.data))
        lassoR.Close()
    }
}

func qUpdate(update *ClientCmd, crchan chan<- *ClientCmd) {
    crchan <- update
}

// reads the data from service to forward to client through lassoed connections
func serviceToTunnelHandler(c Conn, client int64, pachan chan *Shot, padchan chan []byte,
    pls int, tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) {

    // start the rawMaker
    go rawMaker(pachan, padchan)

    // log.Printf("shot server id is: %v", timeBytes(shot.client))
    seq := uint32(0)
    cl := bytesTime(client)
    for {
        shot := Shot{
            client:  cl, // the client id is already decided remotely
            payload: make([]byte, pls),
        }
        // log.Printf("reading from connection...for client %v", timeBytes(shot.client))
        // log.Printf("reading a service payload")
        n, err := readTunnelPayload(c, shot.payload, tichan, tochan, tid, tod)
        // log.Printf("read a service payload, %v", n)
        if err != nil && n == 0 {
            log.Printf("error reading service to tunnel payload: %v", err)
            c.Close()
            break
        }
        shot.ln = n
        shot.ofs = bytesInt(seq)
        seq += uint32(n)
        shot.seq = bytesInt(seq)
        // log.Printf("a shot for the pachan is on its way, ofs: %v", seq - uint32(n))
        pachan <- &shot
    }
}

// the handler for udp only starts a client udp connection to the service to tunnel
func serviceToTunnelHandlerUDP(c *net.UDPConn, client string, pachan chan<- *Shot, pls int) {

    // log.Printf("shot server id is: %v", timeBytes(shot.client))
    seq := uint32(0)
    cl := []byte(client)
    for {
        shot := Shot{
            client:  cl, // the client id is already decided remotely
            payload: make([]byte, pls),
        }
        // log.Printf("reading from connection...for client %v", timeBytes(shot.client))
        n, err := c.Read(shot.payload)
        // log.Printf("read some stuff!")
        if err != nil && n == 0 {
            if err == io.EOF {
                log.Printf("breaking tunnel to service connection: %v", err)
                break
            } else {
                log.Printf("error during service to tunnel connection: %v", err)
                break
            }
        }
        shot.ln = n
        shot.ofs = bytesInt(seq)
        seq += uint32(n)
        shot.seq = bytesInt(seq)
        // log.Printf("channeling shot from service to client UDP")
        pachan <- &shot
    }
}

// keeps the clients list in sync accordingly to connections on the local listener and
// updates from remote peers on the status listener
func syncHandler(addchan <-chan *Client, rmchan <-chan *Client, rSync *string, clients map[int64]bool,
    frmap map[int64]map[uint32]*Shot, flmap map[int64]map[uint32]*Shot, cchan <-chan *ClientCmd, ccon map[int64]Conn,
    forward *string, pachan chan *Shot, padchan chan []byte, retries bool, rtmap map[int64]chan *ShotRtr, mtx *sync.Mutex,
    pls int, tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration,
    st int, cq <-chan Conn, newc chan<- bool, schan <-chan bool) {
    for {
        // loop over channels events to keep the list of persistent connections updated
        select {
        case client := <-addchan:
            ct := timeBytes(client.client)
            qlen := st * 100 // the length of the buffered retry shots

            clients[ct] = true
            ccon[ct] = client.conn
            frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
            flmap[ct] = make(map[uint32]*Shot) // init local payloads map
            if retries {
                rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
                go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
            }
            log.Printf("payloads maps for %v initialized", ct)

            update := ClientCmd{
                cmd:    []byte{1},
                client: client.client,
            }
            sendClientUpdate(&update, rSync)
        case client := <-rmchan:
            ct := timeBytes(client.client)

            go clearConn(clients, ccon, frmap, rtmap, ct)

            update := ClientCmd{
                cmd:    []byte{0},
                client: client.client,
            }
            sendClientUpdate(&update, rSync)
        case update := <-cchan: // this case is basically for server requests from remote clients
            ct := timeBytes(update.client)

            switch {
            case update.cmd[0] == 0:
                go clearConn(clients, ccon, frmap, rtmap, ct)
            case update.cmd[0] == 1:
                clients[ct] = true
                qlen := st * 100 // the length of the buffered retry shots

                // open local connection to reflect the synced clients list
                // the address is forwardbecause we can only receive new connection
                // updates from clients asking for a connection on the service tunneled
                // through this peer.
                // conn, err := net.Dial("tcp", *forward)
                forwardAddr, _ := net.ResolveTCPAddr("tcp", *forward)
                conn, err := net.DialTCP("tcp", nil, forwardAddr)
                if err != nil {
                    log.Printf("error syncying new connection: %v", err)
                }
                ccon[ct] = conn
                frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
                flmap[ct] = make(map[uint32]*Shot) // init local payloads map
                if retries {
                    rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
                    go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
                }

                go serviceToTunnelHandler(ccon[ct], ct, pachan, padchan, pls, tichan, tochan, tid, tod)
                log.Printf("payloads maps for %v initialized", ct)
                // log.Printf("handled status case update, frmap : %v, flmap: %v", len(frmap), len(flmap))
            case update.cmd[0] == 2: // this is a retry command
                // log.Printf("received a retry command")
                go refling(rtmap, update.client, update.data, cq, newc)

            }
            // log.Printf("map updated!: \n")
            // spew.Dump(clients)
        }
    }
}

func syncHandlerUDP(addchan <-chan *ClientUDP, rmchan <-chan *ClientUDP, rSync *string, clients map[string]*net.UDPAddr,
    frmap map[string]map[uint32]*Shot, flmap map[string]map[uint32]*Shot, cchan <-chan *ClientCmd, ccon map[string]*net.UDPConn,
    forward *string, pachan chan<- *Shot, retries bool, rtmap map[string]chan *ShotRtr, mtx *sync.Mutex, pls int,
    st int, cq <-chan Conn, newc chan<- bool, schan <-chan bool) {
    for {
        // loop over channels events to keep the list of persistent connections updated
        select {
        case client := <-addchan:
            ct := string(client.client)
            // when creating the index key locally need to make sure it is 21 bytes long
            lnCt := len(ct)
            if lnCt != 21 {
                ct = ct + strings.Repeat("\x00", 21-lnCt)
            }
            qlen := st * 100 // the length of the buffered retry shots

            // local writing for UDP is handled by the listener connection
            // log.Printf("local udp client to tunnel connection with ct %v, len %v", ct, len(ct))
            frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
            flmap[ct] = make(map[uint32]*Shot) // init local payloads map
            if retries {
                rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
                go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
            }
            log.Printf("payloads maps for %v initialized", ct)

            update := ClientCmd{
                cmd:    []byte{1},
                client: client.client,
            }
            sendClientUpdateUDP(update, rSync)
        case client := <-rmchan:
            ct := string(client.client)

            go clearConnUDP(clients, ccon, frmap, rtmap, ct)

            update := ClientCmd{
                cmd:    []byte{0},
                client: client.client,
            }
            sendClientUpdateUDP(update, rSync)
        case update := <-cchan: // this case is basically for server requests from remote clients
            ct := string(update.client)

            switch {
            case update.cmd[0] == 0:
                go clearConnUDP(clients, ccon, frmap, rtmap, ct)
            case update.cmd[0] == 1:
                clients[ct], _ = net.ResolveUDPAddr("udp", strings.TrimSuffix(string(update.client), "\x00"))
                udpForward, _ := net.ResolveUDPAddr("udp", *forward)
                qlen := st * 100 // the length of the buffered retry shots

                // open local connection to reflect the synced clients list
                // the address is forwardbecause we can only receive new connection
                // updates from clients asking for a connection on the service tunneled
                // through this peer.
                conn, err := net.DialUDP("udp", nil, udpForward)
                if err != nil {
                    log.Printf("error dialing udp tunnel->service connection: %v", err)
                }
                ccon[ct] = conn
                // log.Printf("putting conn inside key %v, len %v", ct, len(ct))
                frmap[ct] = make(map[uint32]*Shot) // init remote payloads map
                flmap[ct] = make(map[uint32]*Shot) // init local payloads map
                if retries {
                    rtmap[ct] = make(chan *ShotRtr, qlen) // init buffered payloads map
                    go rtFlusher(schan, rtmap[ct], qlen)  // start the flusher for the client payloads buffer
                }

                go serviceToTunnelHandlerUDP(ccon[ct], ct, pachan, pls)
                log.Printf("payloads maps for %v initialized", ct)
                // log.Printf("handled status case update, frmap : %v, flmap: %v", len(frmap), len(flmap))
            case update.cmd[0] == 2: // this is a retry command
                // log.Printf("received a retry command")
                go reflingUDP(rtmap, update.client, update.data, cq, newc)

            }
            // log.Printf("map updated!: \n")
            // spew.Dump(clients)
        }
    }
}

func sendClientUpdate(update *ClientCmd, rSync *string) {
    conn, err := net.Dial("tcp", *rSync)
    if err != nil {
        log.Fatalf("error connecting to remote status server: %v", err)
    }
    // make a slice big enough 1 + 8
    dst := make([]byte, 9)
    // concatenate shot fieds
    copy(dst[0:], update.cmd)
    copy(dst[1:], update.client)
    // log.Printf("client update data len: %v", len(dst))
    conn.Write(dst)
    // log.Printf("client update wrote %v, err: %v", n, e)
    conn.Close()
    // log.Printf("closed client update with error: %v", e)
}

func sendClientUpdateUDP(update ClientCmd, rSync *string) {
    conn, err := net.Dial("tcp", *rSync)
    if err != nil {
        log.Fatalf("error connecting to remote status server: %v", err)
    }
    // make a slice big enough 1 + 21
    dst := make([]byte, 22)
    // concatenate shot fieds
    copy(dst[0:], update.cmd)
    copy(dst[1:], update.client)
    // log.Printf("client update data len: %v", len(dst))
    conn.Write(dst)
    // log.Printf("client update wrote %v, err: %v", n, e)
    conn.Close()
    // log.Printf("closed client update with error: %v", e)
}

// write the shot to the connection and handle possible retries
// func write(dst []byte, c Conn, rtchan chan<- ShotRtr) {
// c.Write(dst)
// c.Close() // connection must be closed for data to be delivered
// }

// func rewritesHandler(rtchan chan ShotRtr) {
//     for srt := range rtchan {
//         c, e := net.Dial("tcp", *(srt.addr))
//         if e != nil {
//             log.Printf("retry failed: %v", e)
//             continue
//         }
//         write(srt.dst, c, rtchan)
//     }
// }

// send ack and close connection
func sndack(c Conn) {
    log.Printf("sending ack")
    n, e := c.Write([]byte{1})
    if e != nil {
        log.Printf("error sending ack: %v", e)
    }
    log.Printf("sent %v ack", n)
    c.Close()
}

// receive ack and queue retries channel if failed
func rcvack(c Conn, dst []byte, newc chan<- bool, cq <-chan Conn) {
    ack := make([]byte, 1)
    log.Printf("reading the ack")
    n, err := c.Read(ack)
    log.Printf("read the ack")
    if n != 8 && err != nil && err != io.EOF {
        log.Printf("ack failed n: %v, err: %v", n, err)
        // shoot again
        // shoot(dst, newc, cq)
        // addr := c.RemoteAddr().String()
        // rtchan <- ShotRtr{
        // dst:  dst,
        // ofs: bytesInt(ofs),
        // }
        return
    }
    log.Printf("ack succeded")
    c.Close()
}

func clearConn(clients map[int64]bool, ccon map[int64]Conn, shotsMap map[int64]map[uint32]*Shot, retriesMap map[int64]chan *ShotRtr, ct int64) {
    time.Sleep(5 * time.Second)
    // wait until the shots map is empty
    for len(shotsMap[ct]) > 0 {
        time.Sleep(1 * time.Second)
    }
    // remote client from client list
    delete(clients, ct)
    // close local connection to reflect the synced clients list
    ccon[ct].Close()
    delete(ccon, ct)
    // delete payloads map
    delete(shotsMap, ct)
    // delete retries channel
    delete(retriesMap, ct)
    // log.Printf("connection %v cleared", ct)
}

func clearConnUDP(clients map[string]*net.UDPAddr, ccon map[string]*net.UDPConn,
    shotsMap map[string]map[uint32]*Shot, retriesMap map[string]chan *ShotRtr, ct string) {
    time.Sleep(5 * time.Second)
    // wait until the shots map is empty
    for len(shotsMap[ct]) > 0 {
        time.Sleep(1 * time.Second)
    }
    // remote client from client list
    delete(clients, ct)
    // close local connection to reflect the synced clients list
    // for UDP only the server end has a connection since
    // on the client side the udp listener does not return connections
    if ccon[ct] != nil {
        ccon[ct].Close()
        delete(ccon, ct)
    }
    // delete payloads map
    delete(shotsMap, ct)
    // delete retries channel
    delete(retriesMap, ct)
    // log.Printf("connection %v cleared", ct)
}

// timeout for cutting payload reads
func tocker(tochan chan<- bool, to time.Duration) {
    for {
        time.Sleep(to * time.Millisecond)
        tochan <- true
        // log.Printf("tock")
    }
}

// time unit for every read
func ticker(tichan chan<- bool, ti time.Duration) {
    for {
        time.Sleep(ti * time.Millisecond)
        tichan <- true
        // log.Printf("tick")
    }
}

// handle the connection read timeout
func ckRead(n int, err error) bool {
    if e, ok := err.(net.Error); ok && e.Timeout() {
        return true // timeout
    } else if n == 0 || err != nil {
        // log.Printf("empty payload, error: %v", e)
        return false
    }
    return true
}

// read payload from client or from service following the timeout
func readTunnelPayload(c Conn, pl []byte, tichan <-chan bool, tochan <-chan bool, tid time.Duration, tod time.Duration) (int, error) {
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

    for {
        select {
        case <-tichan:
            if tn != 0 {
                c.SetReadDeadline(time.Now().Add(tod))
            }
            // log.Printf("reading from client")
            n, e = c.Read(pl[tn:])
            // log.Printf("read from client %v", n)
            if !ckRead(n, e) {
                return tn, e
            }
            tn += n
            time.Sleep(tid)
        case <-tochan:
            // log.Printf("returning from tock")
            return tn, nil
        }
    }

}

// read payload from lasso for dealing with mtu
func readLassoPayload(c Conn, pl []byte, tn *int, newc chan<- bool) bool {
    var n, tnv int
    maxRead := len(pl)
    for tnv < maxRead { // read until the slice is filled
        // log.Printf("conn: %v , payload length: %v", c.RemoteAddr().String(), len(pl))
        // n, e = c.Read(pl[tnv:])
        if !cRead(c, pl[tnv:], &n, newc) {
            if tnv == 0 {
                go c.Close()
                return false
            }
            *tn = tnv
            return true
        }
        // if n < pn { // if the last read is less than the previous, it is the last part of the payload
        // *tn = tnv
        // return true
        // }
        // pn = n
        tnv += n

        // log.Printf("conn: %v, payload read: %v", c.RemoteAddr().String(), pN)
        // switch {
        // case tnv == 0 && e != nil:
        // log.Printf("error reading payload from %v, %v", c.RemoteAddr(), e)
        // cClose(c, newc)
        // return false
        // case tnv != 0:
        // return true
        // default:
        // log.Printf("empty payload read from %v", c.RemoteAddr())
        // cClose(c, newc)
        // return false
        // }

    }
    *tn = tnv
    return true
}

// read shot payload without queuing up new connections
func readShotPayloadNoQ(c Conn, pl []byte, tn *int) bool {
    var n, tnv int
    maxRead := len(pl)
    for tnv < maxRead { // read until the slice is filled
        // log.Printf("conn: %v , payload length: %v", c.RemoteAddr().String(), len(pl))
        // n, e = c.Read(pl[tnv:])
        if !cReadNoQ(c, pl[tnv:], &n, false) {
            if tnv == 0 {
                go c.Close()
                log.Printf("uh oh, empty shot payload read...")
                return false
            }
            *tn = tnv
            return true
        }
        // if n < pn { // if the last read is less than the previous, it is the last
        // *tn = tnv
        // log.Printf("returning from last payload check, %v %v %v %v", pn, n, tnv, *tn)
        // return true
        // }
        // log.Printf("conn: %v, payload read: %v", c.RemoteAddr().String(), pN)
        // pn = n
        tnv += n
        // switch {
        // case tnv == 0 && e != nil:
        // 	log.Printf("error reading payload from %v, %v", c.RemoteAddr(), e)
        // 	go c.Close()
        // 	return false
        // case tnv != 0:
        // 	return true
        // default:
        // 	log.Printf("empty payload read from %v", c.RemoteAddr())
        // 	c.Close()
        // 	return false
        // }
    }
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
    } else {
        return int64(binary.LittleEndian.Uint64(ba))
    }
}

func bytesInt(i uint32) []byte {
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, i)
    return bs
}

func intBytes(ba []byte) uint32 {
    return binary.LittleEndian.Uint32(ba)
}
