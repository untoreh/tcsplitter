package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/urfave/cli"
	"gopkg.in/fatih/pool.v2"
)

// The Shot is each connection including the time point of execution, the payload and
// its (optional) its length
type Shot struct {
	client  []byte // int64
	ofs     []byte // uint32
	seq     []byte // uint32
	payload []byte
	ln      int
}
type ClientCmd struct {
	cmd    []byte // bool
	client []byte // int64
}
type Client struct {
	client []byte // int64
	conn   net.Conn
}

func main() {

	var pls, listen, lFling, rFling, forward, lLasso, rLasso, lSync, rSync string
	var rmtx, lmtx sync.Mutex
	// var rmtx sync.Mutex

	app := cli.NewApp()

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "payload",
			Value:       "4096",
			Usage:       "size of the payload for each connection in bytes",
			Destination: &pls,
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
		fchan := make(chan Shot)
		// channel for received flinged shots from remote
		rfchan := make(chan Shot)

		// channels for local connections management
		addchan := make(chan Client)
		rmchan := make(chan Client)
		// channel for lassos connections
		laschan := make(chan net.Conn)
		// channel for shots to be caught by lassos
		pachan := make(chan Shot)
		// channel for shots caught from lassos
		cachan := make(chan Shot)
		// holds lasso connections payloads
		flmap := make(map[int64]map[uint32]Shot)

		// holds connections updates
		cchan := make(chan ClientCmd)
		// holds clients connections ids
		clients := make(map[int64]bool)
		// holds clients connections objects
		ccon := make(map[int64]net.Conn)
		// holds clients connections payloads
		frmap := make(map[int64]map[uint32]Shot)

		// payload size
		pls, _ := strconv.Atoi(pls)

		go syncServer(errchan, &lSync, clients, cchan)
		go clientServer(errchan, &listen, fchan, addchan, rmchan, pls)
		if lFling != "0" {
			go flingServer(errchan, &lFling, rfchan, pls)
		}
		if lLasso != "0" {
			go lassoServer(errchan, &lLasso, laschan)
		}

		go syncHandler(addchan, rmchan, &rSync, clients, frmap, flmap, cchan, ccon, &forward, pachan, &rmtx, pls)
		go dispatch(rfchan, ccon, frmap, &rmtx)
		go dispatch(cachan, ccon, flmap, &lmtx)
		go fling(fchan, &rFling)
		if rLasso != "0" {
			go lasso(&rLasso, cachan, pls)
		}
		go toLassos(pachan, laschan)

		return <-errchan
	}

	app.Run(os.Args)
}

// listens to connections from client to be tunneled
func clientServer(errchan chan<- error, addr *string, fchan chan<- Shot, addchan chan<- Client, rmchan chan<- Client, pls int) {

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
		go handleClientToTunnel(conn, fchan, addchan, rmchan, pls)
	}
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
		go handleTunnelToTunnel(conn, rfchan, pls)
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

// sends shots to flingServer
func fling(fchan <-chan Shot, lFling *string) {
	// addr := strings.Join([]string{ip, strconv.Itoa(port)}, ":")
	// conn, err := net.Dial("tcp", addr)

	for {
		shot := <-fchan
		conn, err := net.Dial("tcp", *lFling)

		if err != nil {
			log.Printf("error opening fling: %v", err)
		} else {
			// log.Printf("flinging stuff...")
			// log.Printf("%v", fling.payload)
			dst := append(shot.client, shot.ofs...)
			dst = append(dst, shot.seq...)
			log.Printf("flinging payload, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
			// log.Printf("shooting this payload...%v", shot.payload)
			_, err := conn.Write(append(dst, shot.payload[0:shot.ln]...))
			if err != nil {
				log.Printf("error writing to fling: %v", err)
			}
			// log.Printf("wrote %v bytes to fling", n)
			conn.Close()
		}
	}
}

// throw a bunch of connections for catching incoming data to the lassoServer
func lasso(rLasso *string, cachan chan<- Shot, pls int) {

	st := 100 // throws
	cthrow := make(chan bool)
	factory := func() (net.Conn, error) { return net.Dial("tcp", *rLasso) }
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
		go throw(&p, cthrow, cachan, pls)
		t++
	}
	// keep up throws
	for range cthrow {
		// log.Printf("throwing a connection...")
		go throw(&p, cthrow, cachan, pls)
	}
}

// throw a single connection waiting for reading data
func throw(p *pool.Pool, cthrow chan<- bool, cachan chan<- Shot, pls int) {
	c, err := (*p).Get()
	if err != nil {
		log.Printf("couldn't get a connection from the pool")
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
		if pc, ok := c.(*pool.PoolConn); ok {
			pc.MarkUnusable()
			pc.Close()
		}
		cthrow <- true
		return
	}
	// log.Printf("this is time: %v", timeBytes(shot.client))
	// log.Printf("read shot.time %v", shot.time)
	oN, oErr := c.Read(shot.ofs)
	if oErr != nil || oN == 0 {
		log.Printf("throw, ofs read error: %v", oErr)
		if pc, ok := c.(*pool.PoolConn); ok {
			pc.MarkUnusable()
			pc.Close()
		}
		cthrow <- true
		return
	}
	sN, sErr := c.Read(shot.seq)
	if sErr != nil || sN == 0 {
		log.Printf("throw, seq read error: %v", sErr)
		if pc, ok := c.(*pool.PoolConn); ok {
			pc.MarkUnusable()
			pc.Close()
		}
		cthrow <- true
		return
	}
	pN, pErr := readPayload(c, shot.payload)
	// log.Printf("read throw payload...")
	if pErr != nil && pN == 0 {
		log.Printf("throw, payload read error: %v, pN: %v", pErr, pN)
		if pc, ok := c.(*pool.PoolConn); ok {
			pc.MarkUnusable()
			pc.Close()
		}
		cthrow <- true
		return
	}

	// log.Printf("read payload... %v", len(shot.payload))
	shot.ln = pN

	log.Printf("channeling lassoed shot")
	cachan <- shot
	// log.Printf("received a shot from a lasso...")

	// close connection for opening a new one
	if pc, ok := c.(*pool.PoolConn); ok {
		pc.MarkUnusable()
		pc.Close()
		cthrow <- true
	}
}

// prepare received shots to be forwarded
func dispatch(shotsChan <-chan Shot, connMap map[int64]net.Conn, clientOfsMap map[int64]map[uint32]Shot, mtx *sync.Mutex) {
	cofs := make(map[int64]uint32) // holds the current seq of each connection
	for {
		// mtx.Lock()
		shot := <-shotsChan
		ct := timeBytes(shot.client)
		ofs := intBytes(shot.ofs)
		log.Printf("dispatching shot with ofs: %v", ofs)
		st := time.Duration(10)
		for clientOfsMap[ct] == nil {
			log.Printf("waiting for connection to get established", len(clientOfsMap))
			time.Sleep((st + 10) * time.Millisecond)
		}
		clientOfsMap[ct][ofs] = shot
		// log.Printf("forwarding shots received from lassos...")
		// mtx.Lock()
		cofs = forward(ct, cofs, clientOfsMap, connMap)
		// mtx.Unlock()
	}
}

// forward shots in an ordered manner to the right client
func forward(ct int64, cofs map[int64]uint32, clientOfsMap map[int64]map[uint32]Shot, connMap map[int64]net.Conn) map[int64]uint32 {
	for {
		// log.Printf("LOCAL forwarding...from tunneled server to client")
		ofs := cofs[ct]
		var err error
		// log.Printf("frmap seq keys for client are...\n")
		if shot, ready := clientOfsMap[ct][ofs]; ready {
			_, err = connMap[ct].Write(shot.payload[0:shot.ln])
			if err != nil { // something wrong with the connection
				log.Printf("forward stopped: %v", err)
				return cofs
			} else {
				log.Printf("forwarding successful, ofs: %v, seq: %v", intBytes(shot.ofs), intBytes(shot.seq))
				delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
				cofs[ct] = intBytes(shot.seq)
			}
		} else {
			log.Printf("shot not ready...")
			// time.Sleep(1 * time.Second)
			// log.Printf("fixed payload not found, trying reduced...")
			// the last payload might not be fixed size
			// for lastshot := range clientOfsMap[ct] { // get the last seq from the map
			// 	shot := clientOfsMap[ct][lastshot] // get the shot of such seq
			// 	// log.Printf("var is...: %v", conn[ct])
			// 	n, err := connMap[ct].Write(shot.payload[0:shot.ln])
			// 	if err != nil {
			// 		log.Printf("forward stopped, error writing last payload: %v", err)
			// 		cofs[ct] = ofs - uint32(4096)
			// 		return cofs
			// 	} else {
			// 		log.Printf("reduced forwarding successful, length: %v, seq: %v", shot.ln, ofs -uint32(4096-n))
			// 		cofs[ct] = ofs - uint32(4096-n)
			// 		delete(clientOfsMap[ct], cofs[ct]) // clear the forwarded shot, loop again
			// 		return cofs
			// 	}
			// }
			break
		}
	}
	return cofs
}

// manage connections from clients to be tunneled
func handleClientToTunnel(c net.Conn, fchan chan<- Shot, addchan chan<- Client, rmchan chan<- Client, pls int) {

	// log.Printf("the bytes right now are : %v ", t)
	cl := Client{
		client: bytesTime(0),
		conn:   c,
	}

	addchan <- cl
	defer func() {
		rmchan <- cl
	}()

	// log.Printf("shot client id is: %v", timeBytes(shot.client))
	seq := uint32(0)
	for {
		shot := Shot{
			client:  cl.client,
			payload: make([]byte, pls),
		}
		n, err := c.Read(shot.payload)
		// log.Printf("Reading stuff..")
		// log.Printf("read payload is...\n%v", string(shot.payload))
		if err != nil || n == 0 {
			log.Printf("Closing local connection, error: %v", err)
			c.Close()
			break
		}
		shot.ln = n
		shot.ofs = bytesInt(seq)
		seq += uint32(n)
		shot.seq = bytesInt(seq)
		// log.Printf("the shot to be sent has this payload (length %v)...%v", shot.ln, shot.payload)
		fchan <- shot
	}
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
		log.Printf("client id read error: %v", tErr)
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
	// log.Printf("channeling shot")
	// log.Printf("the shot to be forwarded has this payload: %v", shot.payload)
	rfchan <- shot
	// log.Printf("received a fling...")
	c.Close()
}

// manages connections on the client syncServer for updating the clients list
func handleSyncConnection(c net.Conn, clients map[int64]bool, cchan chan<- ClientCmd) {
	update := ClientCmd{
		cmd:    make([]byte, 1),
		client: make([]byte, 8),
	}
	_, err := c.Read([]byte(update.cmd))
	if err != nil {
		log.Printf("status command read error: %v", err)
		c.Close()
	}
	_, err = c.Read([]byte(update.client))
	if err != nil {
		log.Printf("status client read error: %v", err)
		c.Close()
	}
	cchan <- update
}

// put lassos in the lassos channel
func handleLassos(c net.Conn, lassos chan<- net.Conn) {
	lassos <- c
	return
}

// sends the shots to waiting lasso connections
func toLassos(pachan <-chan Shot, laschan <-chan net.Conn) {
	for {
		shot := <-pachan
		lasso := <-laschan // get a lasso
		dst := append(shot.client, shot.ofs...)
		dst = append(dst, shot.seq...)
		lasso.Write(append(dst, shot.payload[0:shot.ln]...))
		log.Printf("wrote a shot to a lasso ofs: %v, seq:: %v", intBytes(shot.ofs), intBytes(shot.seq))
		lasso.Close()
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

// keeps the clients list in sync accordingly to connections on the local listener and
// updates from remote peers on the status listener
func syncHandler(addchan <-chan Client, rmchan <-chan Client, rSync *string, clients map[int64]bool,
	frmap map[int64]map[uint32]Shot, flmap map[int64]map[uint32]Shot, cchan <-chan ClientCmd, ccon map[int64]net.Conn,
	forward *string, pachan chan<- Shot, mtx *sync.Mutex, pls int) {
	for {
		// loop over channels events to keep the list of persistent connections updated
		select {
		case client := <-addchan:
			ct := timeBytes(client.client)

			clients[ct] = true
			ccon[ct] = client.conn
			frmap[ct] = make(map[uint32]Shot) // init remote payloads map
			flmap[ct] = make(map[uint32]Shot) // init local payloads map
			log.Printf("payloads maps for %v initialized", ct)

			update := ClientCmd{
				cmd:    []byte{1},
				client: client.client,
			}
			sendClientUpdate(update, rSync)
		case client := <-rmchan:
			ct := timeBytes(client.client)

			go clearConn(clients, ccon, frmap, ct)

			update := ClientCmd{
				cmd:    []byte{0},
				client: client.client,
			}
			sendClientUpdate(update, rSync)
		case update := <-cchan: // this case is basically for server requests from remote clients
			ct := timeBytes(update.client)

			if update.cmd[0] == 0 {
				go clearConn(clients, ccon, frmap, ct)
			} else {
				clients[ct] = true
				// open local connection to reflect the synced clients list
				// the address is forwardbecause we can only receive new connection
				// updates from clients asking for a connection on the service tunneled
				// through this peer.
				conn, err := net.Dial("tcp", *forward)
				if err != nil {
					log.Printf("error syncying new connection: %v", err)
				}
				ccon[ct] = conn
				frmap[ct] = make(map[uint32]Shot) // init remote payloads map
				flmap[ct] = make(map[uint32]Shot) // init local payloads map

				go serviceToTunnelHandler(ccon[ct], ct, pachan, pls)
				log.Printf("payloads maps for %v initialized", ct)

				// time.Sleep(500 * time.Millisecond)
				// go serverDispatch(rfchan, forward, ccon, frmap) // startserverDispatch
				log.Printf("handled status case update, frmap length: %v", len(frmap))
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
	conn.Write(append(update.cmd, update.client...))
	conn.Close()
}

func clearConn(clients map[int64]bool, ccon map[int64]net.Conn, frmap map[int64]map[uint32]Shot, ct int64) {
	time.Sleep(5 * time.Second)
	// remote client from client list
	delete(clients, ct)
	// close local connection to reflect the synced clients list
	ccon[ct].Close()
	delete(ccon, ct)
	// delete payloads map
	delete(frmap, ct)
	// log.Printf("connection %v cleared", ct)
}

func readPayload(c net.Conn, pl []byte) (int, error) {
	var pErr error
	var totPN int
	for totPN < len(pl) { // read until the slice is filled
		pN, pErr := c.Read(pl)
		if pErr == io.EOF || pN == 0 {
			// log.Printf("breaking remote reading")
			return totPN, pErr
		}
		if pErr != nil {
			log.Fatalf("RP payload read error: %v", pErr)
		}
		// log.Printf("payload is...\n %v", shot.payload)
		totPN += pN
	}
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
