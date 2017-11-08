package main

import (
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
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
	client []byte // 21bytes string (ipv4:port) for UDP
	conn   *net.UDPConn
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

	shot.ln = uint32(n)

	// log.Printf("channeling lassoed shot for udp")
	cachan <- &shot
	cClose(c, newc)
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

	shot.ln = uint32(n)

	// go sndack(c)
	rfchan <- &shot
	go c.Close()
	// log.Printf("channeled a shot")
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
		shot.ln = uint32(n)
		shot.ofs = bytesInt(seq)
		seq += uint32(n)
		shot.seq = bytesInt(seq)
		// log.Printf("channeling shot from service to client UDP")
		pachan <- &shot
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
