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

// write the fling to the connection blindly
func shootRtr(dst []byte, newc chan<- bool, cq <-chan Conn) {
	c := <-cq
	c.Write(dst)
	c.Close()
	log.Printf("shot flung len: %v", len(dst))
	newc <- true
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

// put reverse lassos in the reverse lassos channel
func handleLassosR(c Conn, lassosR chan<- Conn) {
	lassosR <- c
	return
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
