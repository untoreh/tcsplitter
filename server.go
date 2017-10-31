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
	"gopkg.in/fatih/pool.v2"
)

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
	conn   net.Conn
}

type ClientUDP struct {
	client []byte // 21bytes string (ipv4:port) for UDP
	conn   *net.UDPConn
}

func main() {
	var prctl, pls, conns, lassoes,listen, lFling, rFling, forward, lLasso, rLasso, lLassoR, rLassoR, lSync, rSync string
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
		// general errors channel
		errchan := make(chan error)
		// channel for flinging
		fchan := make(chan []byte)
		// channel for received flinged shots from remote
		rfchan := make(chan Shot)

		// channel for lassos connections
		laschan := make(chan net.Conn)
		// channel for reverse lassos connections
		larchan := make(chan net.Conn)

		// channel for shots to be caught by lassos
		pachan := make(chan Shot)
		// channel for shots caught from lassos
		cachan := make(chan Shot)

		// holds direct client commands (like connection sync/updates)
		cchan := make(chan ClientCmd)
		// holds reverse client commands
		crchan := make(chan ClientCmd)

		// payload size
		pls, _ := strconv.Atoi(pls)
		conns, _ := strconv.Atoi(conns)
		lassoes, _ := strconv.Atoi(lassoes)

		// conn queue channel for flings
		cq := make(chan net.Conn, conns)
		// new connections requests for conn queue channel (channel length to avoid write blocking)
		newc := make(chan bool, 2)
		// channel for issueing flushes of the retries channel
		schan := make(chan bool, 2)

		switch prctl {
		case "tcp":
			// channels for local connections management
			addchan := make(chan Client)
			rmchan := make(chan Client)
			// holds clients connections buffered payloads for retries
			rtmap := make(map[int64]chan ShotRtr)
			// holds clients connections ids
			clients := make(map[int64]bool)
			// holds clients connections payloads
			frmap := make(map[int64]map[uint32]Shot)
			// holds lasso connections payloads
			flmap := make(map[int64]map[uint32]Shot)
			// holds clients connections objects
			ccon := make(map[int64]net.Conn)

			go syncServer(errchan, &lSync, clients, cchan)
			go clientServer(errchan, &listen, fchan, rtmap, addchan, rmchan, schan, pls)
			go syncHandler(addchan, rmchan, &rSync, clients, frmap, flmap, cchan, ccon, &forward, pachan, rtmap, &rmtx, pls, conns, cq, newc, schan)
			go dispatch(rfchan, crchan, ccon, frmap, &rmtx) // clientToServer dispatcher
			go dispatch(cachan, cchan, ccon, flmap, &lmtx)  // serverToClient dispatcher
			if rLasso != "0" {
				go lasso(&rLasso, cachan, pls, lassoes)
			}
			if lLasso != "0" {
				go lassoServer(errchan, &lLasso, laschan)
				go toLassos(pachan, laschan)
			}
			if lFling != "0" {
				go flingServer(errchan, &lFling, rfchan, pls)
			}
		case "udp":
			// channels for local connections management
			addchan := make(chan ClientUDP)
			rmchan := make(chan ClientUDP)
			// holds clients connections buffered payloads for retries
			rtmap := make(map[string]chan ShotRtr)
			// holds clients connections ids
			clients := make(map[string]*net.UDPAddr)
			// holds clients connections payloads
			frmap := make(map[string]map[uint32]Shot)
			// holds lasso connections payloads
			flmap := make(map[string]map[uint32]Shot)
			// holds clients connections objects, in udp only for server side
			ccon := make(map[string]*net.UDPConn)
			// channel for the single connection of the udp listener
			uchan := make(chan *net.UDPConn)

			go syncServerUDP(errchan, &lSync, clients, cchan)
			go clientServerUDP(errchan, &listen, fchan, rtmap, addchan, rmchan, schan, clients, uchan, pls)
			go syncHandlerUDP(addchan, rmchan, &rSync, clients, frmap, flmap, cchan, ccon, &forward, pachan, rtmap, &rmtx, pls, conns, cq, newc, schan)
			go dispatchUDPServer(rfchan, crchan, ccon, frmap, &rmtx)          // clientToServer dispatcher
			go dispatchUDPClient(uchan, cachan, cchan, clients, flmap, &lmtx) // serverToClient dispatcher
			if rLasso != "0" {
				go lassoUDP(&rLasso, cachan, pls, lassoes)
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
			go fling(fchan, &rFling, cq, newc, conns)
		}
		if lLassoR != "0" {
			go lassoRServer(errchan, &lLassoR, larchan)
			go toLassosR(crchan, larchan)
		}
		if rLassoR != "0" {
			go lassoR(&rLassoR, cchan, pls)
		}

		return <-errchan
	}

	e := app.Run(os.Args)
	log.Printf("tcs terminated, error: %v", e)
}

// listens to connections from client to be tunneled
func clientServer(errchan chan<- error, addr *string, fchan chan<- []byte,
	rtmap map[int64]chan ShotRtr, addchan chan<- Client, rmchan chan<- Client,
	schan chan<- bool, pls int) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleClientToTunnel(conn, fchan, rtmap, addchan, rmchan, schan, pls)
	}
}

func clientServerUDP(errchan chan<- error, addr *string, fchan chan<- []byte,
	rtmap map[string]chan ShotRtr, addchan chan<- ClientUDP, rmchan chan<- ClientUDP,
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
func flingServer(errchan chan<- error, addr *string, rfchan chan<- Shot, pls int) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// log.Printf("handling a received fling")
		go handleTunnelToTunnel(conn, rfchan, pls)
	}

}

// listens to connections from peers for data traffic
func flingServerUDP(errchan chan<- error, addr *string, rfchan chan<- Shot, pls int) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// log.Printf("handling a received fling")
		go handleTunnelToTunnelUDP(conn, rfchan, pls)
	}
}

// listens to lasso connections from clients for reverse data traffic
func lassoServer(errchan chan<- error, addr *string, lassos chan<- net.Conn) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// put the connection in the lassos channel
		// log.Printf("caught a connection!")
		go handleLassos(conn, lassos)
	}

}

// listens to reverse lasso connections from clients for reverse sync requests
func lassoRServer(errchan chan<- error, addr *string, lassosR chan<- net.Conn) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// put the connection in the lassos channel
		go handleLassosR(conn, lassosR)
	}

}

// listens to sync commands requests
func syncServer(errchan chan<- error, addr *string, clients map[int64]bool, cchan chan<- ClientCmd) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleSyncConnection(conn, clients, cchan)
	}
}

// listens to sync commands requests
func syncServerUDP(errchan chan<- error, addr *string, clients map[string]*net.UDPAddr, cchan chan<- ClientCmd) {
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		errchan <- err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleSyncConnectionUDP(conn, clients, cchan)
	}
}

// queue up connections to be used for flinging
func connQueue(addr *string, cq chan<- net.Conn, newc <-chan bool, st int) {

	cn := 0
	for cn < st {
		c, err := net.Dial("tcp", *addr)
		if err != nil {
			log.Printf("failed creating connection for flinging: %v", err)
		}
		cq <- c
		cn++
	}
	for range newc {
		c, err := net.Dial("tcp", *addr)
		if err != nil {
			log.Printf("failed creating connection for flinging: %v", err)
		}
		cq <- c
	}
}

// sends shots to flingServer
func fling(fchan <-chan []byte, rFling *string, cq chan net.Conn, newc chan bool, st int) {
	// addr := strings.Join([]string{ip, strconv.Itoa(port)}, ":")
	// conn, err := net.Dial("tcp", addr)

	// set up connections pool
	go connQueue(rFling, cq, newc, st)

	// start flinging
	for {
		// get the raw shot
		dst := <-fchan
		// get a connection from the pool
		c := <-cq
		go shoot(dst, newc, c)
	}
}

// write the fling to the connection and make sure it is received
func shoot(dst []byte, newc chan<- bool, c net.Conn) {
	c.Write(dst)
	c.Close()
	log.Printf("shot flung len: %v", len(dst))
	newc <- true
}

// skim throw the buffered shots and send the missing shot at offset
func refling(rtmap map[int64]chan ShotRtr, client []byte, data []byte, cq <-chan net.Conn, newc chan<- bool) {
	ct := timeBytes(client)
	ofs := intBytes(data)
	log.Printf("launching refling for offset: %v", ofs)
	for shotR := range rtmap[ct] {
		if shotR.ofs != ofs {
			log.Printf("skipping shot with ofs: %v", shotR.ofs)
			continue
		} else {
			log.Printf("found the right shot to refling")
			c := <-cq
			shoot(shotR.dst, newc, c)
			break
		}
	}
	log.Printf("quitting refling")
}
func reflingUDP(rtmap map[string]chan ShotRtr, client []byte, data []byte, cq <-chan net.Conn, newc chan<- bool) {
	ct := string(client)
	ofs := intBytes(data)
	log.Printf("launching refling for offset: %v", ofs)
	for shotR := range rtmap[ct] {
		if shotR.ofs != ofs {
			log.Printf("skipping shot with ofs: %v", shotR.ofs)
			continue
		} else {
			log.Printf("found the right shot to refling")
			c := <-cq
			shoot(shotR.dst, newc, c)
			break
		}
	}
	log.Printf("quitting refling")
}

// really close a pool connection
func pcClose(c net.Conn, cthrow chan<- bool) {
	if pc, ok := c.(*pool.PoolConn); ok {
		pc.MarkUnusable()
		pc.Close()
	}
	cthrow <- true
}

// throw a bunch of connections for catching incoming data to the lassoServer
func lasso(rLasso *string, cachan chan<- Shot, pls int, lassoes int) {

	cthrow := make(chan bool)
	factory := func() (net.Conn, error) { return net.Dial("tcp", *rLasso) }
	p, err := pool.NewChannelPool(lassoes, lassoes, factory)
	for err != nil {
		log.Printf("pool creation failed, %v", err)
		time.Sleep(1 * time.Second)
		p, err = pool.NewChannelPool(lassoes, lassoes, factory)
	}

	// initiate throws
	t := 0
	for t < lassoes {
		// log.Printf("throwing a connection...")
		go throw(&p, cthrow, cachan, pls)
		t++
	}
	// keep up throws
	for range cthrow {
		// log.Printf("throwing a connection...")
		go throw(&p, cthrow, cachan, pls)
	}
}

// throw a bunch of connections for catching incoming data to the lassoServer
func lassoUDP(rLasso *string, cachan chan<- Shot, pls int, lassoes int) {

	cthrow := make(chan bool)
	factory := func() (net.Conn, error) { return net.Dial("tcp", *rLasso) }
	p, err := pool.NewChannelPool(lassoes, lassoes, factory)
	for err != nil {
		log.Printf("pool creation failed, %v", err)
		time.Sleep(1 * time.Second)
		p, err = pool.NewChannelPool(lassoes, lassoes, factory)
	}

	// initiate throws
	t := 0
	for t < lassoes {
		// log.Printf("throwing a connection...")
		go throwUDP(&p, cthrow, cachan, pls)
		t++
	}
	// keep up throws
	for range cthrow {
		// log.Printf("throwing a connection...")
		go throwUDP(&p, cthrow, cachan, pls)
	}
}

// throw a single connection waiting for reading data
func throw(p *pool.Pool, cthrow chan<- bool, cachan chan<- Shot, pls int) {
	c, err := (*p).Get()
	if err != nil {
		log.Printf("throw, couldn't get a connection from the pool")
	}

	shot := Shot{
		client:  make([]byte, 8),
		ofs:     make([]byte, 4),
		seq:     make([]byte, 4),
		payload: make([]byte, pls),
	}
	// log.Printf("starting reading who is the client from %v", c.LocalAddr().String())
	tN, tErr := c.Read(shot.client)
	if tErr != nil || tN == 0 {
		log.Printf("throw, client id read error: %v", tErr)
		pcClose(c, cthrow)
		return
	}
	log.Printf("throw: read client")
	// log.Printf("this is time: %v", timeBytes(shot.client))
	oN, oErr := c.Read(shot.ofs)
	if oErr != nil || oN == 0 {
		log.Printf("throw, ofs read error: %v", oErr)
		pcClose(c, cthrow)
		return
	}
	log.Printf("throw: read offset")
	sN, sErr := c.Read(shot.seq)
	if sErr != nil || sN == 0 {
		log.Printf("throw, seq read error: %v", sErr)
		pcClose(c, cthrow)
		return
	}
	log.Printf("throw: read seq")
	pN, pErr := readPayload(c, shot.payload)
	// log.Printf("read throw payload...")
	if pErr != nil && pN == 0 {
		log.Printf("throw, payload read error: %v, pN: %v", pErr, pN)
		pcClose(c, cthrow)
		return
	}

	shot.ln = pN

	// log.Printf("channeling lassoed shot")
	cachan <- shot

	log.Printf("closing throw")
	pcClose(c, cthrow)
}

// throw a single connection waiting for reading data
func throwUDP(p *pool.Pool, cthrow chan<- bool, cachan chan<- Shot, pls int) {
	c, err := (*p).Get()
	if err != nil {
		log.Printf("throw, couldn't get a connection from the pool")
	}

	shot := Shot{
		client:  make([]byte, 21),
		ofs:     make([]byte, 4),
		seq:     make([]byte, 4),
		payload: make([]byte, pls),
	}
	// log.Printf("starting reading who is the client from %v", c.LocalAddr().String())
	tN, tErr := c.Read(shot.client)
	if tErr != nil || tN == 0 {
		log.Printf("throw, client id read error: %v", tErr)
		pcClose(c, cthrow)
		return
	}
	// log.Printf("throw: read client")
	// log.Printf("this is time: %v", timeBytes(shot.client))
	oN, oErr := c.Read(shot.ofs)
	if oErr != nil || oN == 0 {
		log.Printf("throw, ofs read error: %v", oErr)
		pcClose(c, cthrow)
		return
	}
	// log.Printf("throw: read offset")
	sN, sErr := c.Read(shot.seq)
	if sErr != nil || sN == 0 {
		log.Printf("throw, seq read error: %v", sErr)
		pcClose(c, cthrow)
		return
	}
	// log.Printf("throw: read seq")
	pN, pErr := readPayload(c, shot.payload)
	// log.Printf("read throw payload...")
	if pErr != nil && pN == 0 {
		log.Printf("throw, payload read error: %v, pN: %v", pErr, pN)
		pcClose(c, cthrow)
		return
	}

	shot.ln = pN

	// log.Printf("channeling lassoed shot for udp")
	cachan <- shot

	// log.Printf("closing throw")
	pcClose(c, cthrow)
}

// lassos for server to client sync requests
func lassoR(rLassoRtr *string, cchan chan<- ClientCmd, pls int) {

	st := 3 // throws
	cthrow := make(chan bool)
	factory := func() (net.Conn, error) { return net.Dial("tcp", *rLassoRtr) }
	p, err := pool.NewChannelPool(st, st, factory)
	for err != nil {
		log.Printf("pool creation failed, %v", err)
		time.Sleep(1 * time.Second)
		p, err = pool.NewChannelPool(st, st, factory)
	}

	// initiate throws
	t := 0
	for t < st {
		// log.Printf("throwing a connection...")
		go throwR(&p, cthrow, cchan, pls)
		t++
	}
	// keep up throws
	for range cthrow {
		// log.Printf("throwing a connection...")
		go throwR(&p, cthrow, cchan, pls)
	}
}
func throwR(p *pool.Pool, cthrow chan<- bool, cchan chan<- ClientCmd, pls int) {
	c, err := (*p).Get()
	if err != nil {
		log.Printf("throwR, couldn't get a connection from the pool")
	}
	update := ClientCmd{
		cmd:    make([]byte, 1),
		client: make([]byte, 8),
		data:   make([]byte, pls),
	}
	log.Printf("starting reading a reverse lasso")
	cN, cErr := c.Read(update.cmd)
	if cErr != nil || cN == 0 {
		log.Printf("throwR, cmd read error: %v", cErr)
		pcClose(c, cthrow)
		return
	}
	tN, tErr := c.Read(update.client)
	if tErr != nil || tN == 0 {
		log.Printf("throwR, client id read error: %v", tErr)
		pcClose(c, cthrow)
		return
	}
	dN, dErr := readPayload(c, update.data)
	// log.Printf("read throwR payload...")
	if dErr != nil && dN == 0 {
		log.Printf("throwR, data read error: %v, dN: %v", dErr, dN)
		pcClose(c, cthrow)
		return
	}
	// log.Printf("got a reverse update request")
	cchan <- update
	// log.Printf("closing throwR")
	pcClose(c, cthrow)
}

// prepare received shots to be forwarded
func dispatch(shotsChan <-chan Shot, crchan chan<- ClientCmd, connMap map[int64]net.Conn, clientOfsMap map[int64]map[uint32]Shot, mtx *sync.Mutex) {
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

// prepare received shots to be forwarded
// io bool true for client->tunnel (uses udp listener conn)
// io bool false for tunnel->service (uses dialed udp net.Conn)
func dispatchUDPServer(shotsChan <-chan Shot, crchan chan<- ClientCmd,
	connMap map[string]*net.UDPConn,
	clientOfsMap map[string]map[uint32]Shot, mtx *sync.Mutex) {
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

func dispatchUDPClient(uchan <-chan *net.UDPConn, shotsChan <-chan Shot, crchan chan<- ClientCmd,
	clients map[string]*net.UDPAddr, clientOfsMap map[string]map[uint32]Shot, mtx *sync.Mutex) {
	cofs := make(map[string]uint32) // holds the current seq of each connection
	rtMap := make(map[string]bool)  // holds the retries
	cf := 0                         // counter for consecutive failed forwarding attempts
	c := <-uchan // wait for the connection of the UDP listener to be established
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
			rtMap[ct] = retryUDP(ct, cofs[ct], crchan)
		}
		// mtx.Unlock()
	}
}

// compose a client cmd request to be written to reverse lassos
func retry(ct int64, ofs uint32, crchan chan<- ClientCmd) bool {
	update := ClientCmd{
		cmd:    []byte{2}, // 2 is the chosen byte n for retries
		client: bytesTime(ct),
		data:   bytesInt(ofs),
	}
	// log.Printf("crafting a retry request")
	crchan <- update
	return true
}

// compose a client cmd request to be written to reverse lassos
func retryUDP(ct string, ofs uint32, crchan chan<- ClientCmd) bool {
	update := ClientCmd{
		cmd:    []byte{2}, // 2 is the chosen byte n for retries
		client: []byte(ct),
		data:   bytesInt(ofs),
	}
	// log.Printf("crafting a retry request")
	crchan <- update
	return true
}

// forward shots in an ordered manner to the right client
func forward(ct int64, cofs map[int64]uint32, cf int, clientOfsMap map[int64]map[uint32]Shot, connMap map[int64]net.Conn) (map[int64]uint32, int) {
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
			log.Printf("shot not ready...")
			cf++ // increase failed forwarding attempts
			break
		}
	}
	return cofs, cf
}

// forward shots in an ordered manner to the right client
func forwardUDPServer(ct string, cofs map[string]uint32, cf int, clientOfsMap map[string]map[uint32]Shot,
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

func forwardUDPClient(c *net.UDPConn, ct string, cofs map[string]uint32, cf int, clientOfsMap map[string]map[uint32]Shot,
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
func handleClientToTunnel(c net.Conn, fchan chan<- []byte, rtmap map[int64]chan ShotRtr, addchan chan<- Client, rmchan chan<- Client, schan chan<- bool, pls int) {

	// log.Printf("the bytes right now are : %v ", t)
	ct := time.Now().UnixNano()
	cl := Client{
		client: bytesTime(ct),
		conn:   c,
	}

	addchan <- cl
	defer func() {
		rmchan <- cl
	}()

	// log.Printf("shot client id is: %v", timeBytes(shot.client))
	seq := uint32(0)
	for {
		payload := make([]byte, pls)
		n, err := c.Read(payload)
		// log.Printf("Reading stuff..")
		// log.Printf("read payload is...\n%v", string(payload))
		if err != nil || n == 0 {
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

		// init retry shot here before we up the seq
		shotR := ShotRtr{
			ofs: seq,
		}

		// continue concat
		seq += uint32(n)
		copy(dst[12:], bytesInt(seq)) // seq
		copy(dst[16:], payload[0:n])  // payload

		// raw shots for the fling channel
		fchan <- dst
		// log.Printf("put a dst into the fling channel")

		// retry shots for the retry channel
		shotR.dst = dst
		rtmap[ct] <- shotR
		schan <- true
	}
}

func handleClientToTunnelUDP(c *net.UDPConn, fchan chan<- []byte,
	rtmap map[string]chan ShotRtr, addchan chan<- ClientUDP, rmchan chan<- ClientUDP,
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
			rmchan <- ClientUDP{client: cl}
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
			addchan <- ClientUDP{client: cl}
			defer func() {
				time.Sleep(30 * time.Second)
				rmchan <- ClientUDP{client: cl}
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
		go queueShotR(rtmap[addr.String()], shotR)
		schan <- true
	}
}

// flush the channel for probably old unneeded shots retries
func rtFlusher(schan <-chan bool, rtc chan ShotRtr, qlen int) {
	for {
		<-schan
		if len(rtc) == qlen {
			<-rtc
		}
	}
}

// queues the retry shot
func queueShotR(rtchan chan ShotRtr, shotR ShotRtr) {
	rtchan <- shotR
}

// manages shots received from the remote end of the tunnel
func handleTunnelToTunnel(c net.Conn, rfchan chan<- Shot, pls int) {
	// buf := make([]byte, 4096)
	shot := Shot{
		client:  make([]byte, 8),
		ofs:     make([]byte, 4),
		seq:     make([]byte, 4),
		payload: make([]byte, pls),
	}
	tN, tErr := c.Read(shot.client)
	if tErr != nil || tN == 0 {
		log.Printf("client id read error: %v", intBytes(shot.client))
		c.Close()
		return
	}
	// log.Printf("this is time: %v", timeBytes(shot.client))
	// log.Printf("read shot.time %v", shot.time)
	oN, oErr := c.Read(shot.ofs)
	if oErr != nil || oN == 0 {
		log.Printf("ofs read error: %v", oErr)
		c.Close()
		return
	}
	sN, sErr := c.Read(shot.seq)
	if sErr != nil || sN == 0 {
		log.Printf("seq read error: %v", sErr)
		c.Close()
		return
	}
	pN, err := readPayload(c, shot.payload)
	if err != nil && pN == 0 {
		log.Printf("TTT payload read error: %v", err)
		c.Close()
		return
	}
	// log.Printf("received shot is: %v", pN+sN+tN+oN)
	shot.ln = pN
	// log.Printf("the shot to be forwarded has this payload: %v", shot.payload)
	rfchan <- shot
	// log.Printf("channeled a shot")
}

// manages shots received from the remote end of the tunnel UDP
func handleTunnelToTunnelUDP(c net.Conn, rfchan chan<- Shot, pls int) {
	// buf := make([]byte, 4096)
	shot := Shot{
		client:  make([]byte, 21),
		ofs:     make([]byte, 4),
		seq:     make([]byte, 4),
		payload: make([]byte, pls),
	}
	tN, tErr := c.Read(shot.client)
	if tErr != nil || tN == 0 {
		log.Printf("client id read error: %v", intBytes(shot.client))
		c.Close()
		return
	}
	// log.Printf("client is this: %v", string(shot.client))
	// log.Printf("read shot.time %v", shot.time)
	oN, oErr := c.Read(shot.ofs)
	if oErr != nil || oN == 0 {
		log.Printf("ofs read error: %v", oErr)
		c.Close()
		return
	}
	sN, sErr := c.Read(shot.seq)
	if sErr != nil || sN == 0 {
		log.Printf("seq read error: %v", sErr)
		c.Close()
		return
	}
	pN, err := readPayload(c, shot.payload)
	if err != nil && pN == 0 {
		log.Printf("TTT payload read error: %v", err)
		c.Close()
		return
	}
	// log.Printf("received shot is: %v", pN+sN+tN+oN)
	shot.ln = pN
	// log.Printf("the shot to be forwarded has this payload: %v", string(shot.payload))
	rfchan <- shot
	// log.Printf("channeled a shot")
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnection(c net.Conn, clients map[int64]bool, cchan chan<- ClientCmd) {
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
	cchan <- update
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnectionUDP(c net.Conn, clients map[string]*net.UDPAddr, cchan chan<- ClientCmd) {
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
	cchan <- update
}

// put lassos in the lassos channel
func handleLassos(c net.Conn, lassos chan<- net.Conn) {
	lassos <- c
	return
}

// put reverse lassos in the reverse lassos channel
func handleLassosR(c net.Conn, lassosR chan<- net.Conn) {
	lassosR <- c
	return
}

// sends the shots to waiting lasso connections
func toLassos(pachan <-chan Shot, laschan <-chan net.Conn) {
	for {
		shot := <-pachan
		lasso := <-laschan // get a lasso
		// make a slice big enough
		dst := make([]byte, (16 + shot.ln))
		// concatenate shot fieds
		copy(dst[0:], shot.client)
		copy(dst[8:], shot.ofs)
		copy(dst[12:], shot.seq)
		copy(dst[16:], shot.payload[0:shot.ln])
		lasso.Write(dst)
		log.Printf("wrote a shot to a lasso ofs: %v, seq:: %v", intBytes(shot.ofs), intBytes(shot.seq))
		lasso.Close()
		// log.Printf("closed lasso with error: %v", e)
	}
}

func toLassosUDP(pachan <-chan Shot, laschan <-chan net.Conn) {
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
func toLassosR(crchan <-chan ClientCmd, larchan <-chan net.Conn) {
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
		lassoR.Write(dst)
		log.Printf("reverse lasso: wrote a command %v, ofs %v", update.cmd, intBytes(update.data))
		lassoR.Close()
	}
}

// reads the data from service to forward to client through lassoed connections
func serviceToTunnelHandler(c net.Conn, client int64, pachan chan<- Shot, pls int) {

	// log.Printf("shot server id is: %v", timeBytes(shot.client))
	seq := uint32(0)
	cl := bytesTime(client)
	for {
		shot := Shot{
			client:  cl, // the client id is already decided remotely
			payload: make([]byte, pls),
		}
		// log.Printf("reading from connection...for client %v", timeBytes(shot.client))
		n, err := c.Read(shot.payload)
		if err != nil || n == 0 {
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
		pachan <- shot
	}
}

// the handler for udp only starts a client udp connection to the service to tunnel
func serviceToTunnelHandlerUDP(c net.Conn, client string, pachan chan<- Shot, pls int) {

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
		if err != nil || n == 0 {
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
		pachan <- shot
	}
}

// keeps the clients list in sync accordingly to connections on the local listener and
// updates from remote peers on the status listener
func syncHandler(addchan <-chan Client, rmchan <-chan Client, rSync *string, clients map[int64]bool,
	frmap map[int64]map[uint32]Shot, flmap map[int64]map[uint32]Shot, cchan <-chan ClientCmd, ccon map[int64]net.Conn,
	forward *string, pachan chan<- Shot, rtmap map[int64]chan ShotRtr, mtx *sync.Mutex, pls int,
	st int, cq <-chan net.Conn, newc chan<- bool, schan <-chan bool) {
	for {
		// loop over channels events to keep the list of persistent connections updated
		select {
		case client := <-addchan:
			ct := timeBytes(client.client)
			qlen := st * 100 // the length of the buffered retry shots

			clients[ct] = true
			// local writing for UDP is handled by the listener connection
			frmap[ct] = make(map[uint32]Shot)    // init remote payloads map
			flmap[ct] = make(map[uint32]Shot)    // init local payloads map
			rtmap[ct] = make(chan ShotRtr, qlen) // init buffered payloads map
			go rtFlusher(schan, rtmap[ct], qlen) // start the flusher for the client payloads buffer
			log.Printf("payloads maps for %v initialized", ct)

			update := ClientCmd{
				cmd:    []byte{1},
				client: client.client,
			}
			sendClientUpdate(update, rSync)
		case client := <-rmchan:
			ct := timeBytes(client.client)

			go clearConn(clients, ccon, frmap, rtmap, ct)

			update := ClientCmd{
				cmd:    []byte{0},
				client: client.client,
			}
			sendClientUpdate(update, rSync)
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
				conn, err := net.Dial("tcp", *forward)
				if err != nil {
					log.Printf("error syncying new connection: %v", err)
				}
				ccon[ct] = conn
				frmap[ct] = make(map[uint32]Shot)    // init remote payloads map
				flmap[ct] = make(map[uint32]Shot)    // init local payloads map
				rtmap[ct] = make(chan ShotRtr, qlen) // init buffered payloads map
				go rtFlusher(schan, rtmap[ct], qlen) // start the flusher for the client payloads buffer

				go serviceToTunnelHandler(ccon[ct], ct, pachan, pls)
				log.Printf("payloads maps for %v initialized", ct)

				log.Printf("handled status case update, frmap : %v, flmap: %v", len(frmap), len(flmap))
			case update.cmd[0] == 2: // this is a retry command
				// log.Printf("received a retry command")
				go refling(rtmap, update.client, update.data, cq, newc)

			}
			// log.Printf("map updated!: \n")
			// spew.Dump(clients)
		}
	}
}

func syncHandlerUDP(addchan <-chan ClientUDP, rmchan <-chan ClientUDP, rSync *string, clients map[string]*net.UDPAddr,
	frmap map[string]map[uint32]Shot, flmap map[string]map[uint32]Shot, cchan <-chan ClientCmd, ccon map[string]*net.UDPConn,
	forward *string, pachan chan<- Shot, rtmap map[string]chan ShotRtr, mtx *sync.Mutex, pls int,
	st int, cq <-chan net.Conn, newc chan<- bool, schan <-chan bool) {
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

			ccon[ct] = client.conn
			// log.Printf("local udp client to tunnel connection with ct %v, len %v", ct, len(ct))
			frmap[ct] = make(map[uint32]Shot)    // init remote payloads map
			flmap[ct] = make(map[uint32]Shot)    // init local payloads map
			rtmap[ct] = make(chan ShotRtr, qlen) // init buffered payloads map
			go rtFlusher(schan, rtmap[ct], qlen) // start the flusher for the client payloads buffer
			// log.Printf("payloads maps for %v initialized", ct)
			// log.Printf("handled status case update, frmap : %v, flmap: %v", len(frmap), len(flmap))

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
				// saddr := strings.TrimSuffix(string(ct), "\x00")
				// udpaddr, err := net.ResolveUDPAddr("udp", saddr)
				// udpaddr, err := net.ResolveUDPAddr("udp", "127.0.0.1")
				if err != nil {
					log.Printf("error dialing udp tunnel->service connection: %v", err)
				}
				// conn, err := net.ListenUDP("udp", udpaddr)
				// if err != nil {
				// log.Printf("error syncying new connection: %v", err)
				// }
				ccon[ct] = conn
				// log.Printf("putting conn inside key %v, len %v", ct, len(ct))
				// clients[ct] = udpaddr
				frmap[ct] = make(map[uint32]Shot)    // init remote payloads map
				flmap[ct] = make(map[uint32]Shot)    // init local payloads map
				rtmap[ct] = make(chan ShotRtr, qlen) // init buffered payloads map
				go rtFlusher(schan, rtmap[ct], qlen) // start the flusher for the client payloads buffer

				go serviceToTunnelHandlerUDP(ccon[ct], ct, pachan, pls)
				// log.Printf("creation update, ct length is: %v", len(ct))
				// log.Printf("payloads maps for %v initialized", ct)
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

func sendClientUpdate(update ClientCmd, rSync *string) {
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
// func write(dst []byte, c net.Conn, rtchan chan<- ShotRtr) {
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

// // send ack and close connection
// func sndack(c net.Conn) {
//     log.Printf("sending ack")
//     n, e := c.Write([]byte{1})
//     if e != nil {
//         log.Printf("error sending ack: %v", e)
//     }
//     log.Printf("sent %v ack", n)
//     c.Close()
// }

// receive ack and queue retries channel if failed
// func rcvack(c net.Conn, dst []byte, rtchan chan<- ShotRtr) {
//     log.Printf("receiving ack")
//     ack := make([]byte, 1)
//     n, err := c.Read(ack)
//     if n != 8 && err != nil && err != io.EOF {
//         log.Printf("ack failed n: %v, err: %v", n, err)
//         addr := c.RemoteAddr().String()
//         rtchan <- ShotRtr{
//             dst:  dst,
//             addr: &addr,
//         }
//         return
//     }
//     log.Printf("ack succeded")
// }

func clearConn(clients map[int64]bool, ccon map[int64]net.Conn, shotsMap map[int64]map[uint32]Shot, retriesMap map[int64]chan ShotRtr, ct int64) {
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
	shotsMap map[string]map[uint32]Shot, retriesMap map[string]chan ShotRtr, ct string) {
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

func readPayload(c net.Conn, pl []byte) (int, error) {
	var pErr error
	var totPN int
	for totPN < len(pl) { // read until the slice is filled
		// log.Printf("conn: %v , payload length: %v", c.RemoteAddr().String(), len(pl))
		pN, pErr := c.Read(pl)
		// log.Printf("conn: %v, payload read: %v", c.RemoteAddr().String(), pN)
		if pErr == io.EOF || pN == 0 {
			// log.Printf("breaking shot payload reading, read %v", totPN)
			return totPN, pErr
		}
		if pErr != nil {
			log.Fatalf("RP payload read error: %v", pErr)
		}
		// log.Printf("payload is...\n %v", shot.payload)
		totPN += pN
	}
	// log.Printf("ending shot payload reading, read %v", totPN)
	return totPN, pErr
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
