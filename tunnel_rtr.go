package main

import (
    "log"
    "net"
    "sync"
    "time"
)

// ShotRtr a shot for the retry channel
type ShotRtr struct {
    ofs uint32
    dst []byte // raw shot
}

// listens to reverse lasso connections from clients for reverse sync requests
func lassoRServer(errchan chan<- error, addr *string, padRchan chan []byte) {
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
        // write queued up raw reverse commands (ClientCmd)
        go handleLassosR(conn, padRchan)
    }

}

// lassos for server to client sync requests
func lassoR(rLassoRtr *string, clcmdchan chan<- *ClientCmd, frg Frags, lassoes int, newc chan bool, tid time.Duration) {

    // a conn queue for lassoes
    cq := make(chan Conn, lassoes)

    go connQueue(rLassoRtr, cq, newc, lassoes)

    // a throw for each connection
    for c := range cq {
        // reverse lassoes don't offer shooting because outgoing data is used for ack
        go throwR(c, newc, clcmdchan, frg, tid)
    }
}

func throwR(c Conn, newc chan<- bool, cchan chan<- *ClientCmd, frg Frags, tid time.Duration) {
    var n int
    update := ClientCmd{
        cmd:    make([]byte, 1),
        client: make([]byte, 8),
        data:   make([]byte, frg.payload),
    }
    // log.Printf("starting reading a reverse lasso")
    if !readClientCmdFields(c, [2][]byte{update.cmd, update.client}, newc) {
        return
    }
    if !readShotPayload(c, update.data, frg, &n, newc, tid) {
        return
    }
    // the length of the data is known apriori because it is decided by the command number
    // log.Printf("channeling a reverse lasso")
    cchan <- &update
    rClose(c, newc)
}

// sends client commands to waiting reverse lasso connections
func handleLassosR(c Conn, padRchan chan []byte) {
    // wait for a raw ClientCmd to write
    // log.Printf("got a reverse command")
    dst := <-padRchan
    n, e := c.Write(dst)
    if n == 0 || e != nil { // requeue command for another reverse lasso
        padRchan <- dst
    } else {
        //log.Printf("reverse lasso: wrote a command %v, ofs %v", update.cmd, intBytes(update.data))
        (*c).CloseWrite()
        (*c).SetReadDeadline(time.Now().Add(10 * time.Second))
        if !rcvack(c) { // if ack failed requeue the command for another reverse lasso
            padRchan <- dst
        }
    }
}

/*** this functions are for shots with payloads retry support ***/

// dispatch function with retry support
func dispatchRtr(shotsChan <-chan *Shot, padRchan chan []byte, connMap map[int64]Conn, clientOfsMap map[int64]map[uint32]*Shot, mtx *sync.Mutex) {
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
        // cofs, cf = forwardRtr(ct, cofs, cf, clientOfsMap, connMap)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            rtMap[ct] = retry(ct, cofs[ct], padRchan)
        }
        // mtx.Unlock()
    }
}

// dispatchUDPClient function with retry support
func dispatchUDPClientRtr(uchan <-chan *net.UDPConn, shotsChan <-chan *Shot, padRchan chan []byte ,
    clients map[string]*net.UDPAddr, clientOfsMap map[string]map[uint32]*Shot, mtx *sync.Mutex, skip int) {
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
        cofs, cf = forwardUDPClient(c, ct, cofs, cf, clientOfsMap, clients, skip)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            log.Printf("retrying a shot...")
            rtMap[ct] = retry(ct, cofs[ct], padRchan)
        }
        // mtx.Unlock()
    }
}

// dispatchUDPServer function with retry support
func dispatchUDPServerRtr(shotsChan <-chan *Shot, padRchan chan []byte,
    connMap map[string]*net.UDPConn,
    clientOfsMap map[string]map[uint32]*Shot, mtx *sync.Mutex, skip int) {
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
        cofs, cf = forwardUDPServer(ct, cofs, cf, clientOfsMap, connMap, skip)
        if cf == 20 { // if failed forwarding for x times request missing shot at current offset
            rtMap[ct] = retry(ct, cofs[ct], padRchan)
        }
        // mtx.Unlock()
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

// compose a client cmd request to be written to lassos
func retry(ct interface{}, ofs uint32, padRchan chan []byte) bool {

    var dst []byte

    switch ct.(type) {
    case int64:
        dst = make([]byte, 13) // 1 + 8 + 4
        copy(dst[0:], []byte{2})
        copy(dst[1:], bytesTime(ct.(int64)))
        copy(dst[9:], bytesInt(ofs))
    case string:
        dst = make([]byte, 26) // 1 + 21 + 4
        copy(dst[0:], []byte{2})
        copy(dst[1:], []byte(ct.(string)))
        copy(dst[22:], bytesInt(ofs))
    }
    padRchan <- dst
    return true
}

// skim throw the buffered shots and send the missing shot at offset
func refling(rtmap map[int64]chan *ShotRtr, client []byte, data []byte, cq <-chan Conn, newc chan<- bool, rmtx *sync.Mutex) {
    ct := timeBytes(client)
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
