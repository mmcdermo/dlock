package dlock_client

import (
	"bytes"
	"strings"
	"net"
	"log"
)

type request struct {
	lock_name   string
	entity      string
	resp_chan   chan bool
}

type Connection struct {
	conn net.Conn
	acquire_requests chan request
	release_requests chan request
	acquire_await map[string]chan bool
	release_await map[string]chan bool
}

func runConnectionWriter(conn *Connection){
	for {
		select {
		case req := <- conn.acquire_requests:
			key := req.lock_name + "_" + req.entity
			if _, ok := conn.acquire_await[key]; !ok {
				//Key doesn't exist, send lock request
				conn.acquire_await[key] = req.resp_chan
				conn.conn.Write([]byte("acquire_lock:"+req.lock_name+":"+req.entity+":\n"))
			} else {
				//Key already exists, trying to double lock
				req.resp_chan <- false
			}

		case req := <- conn.release_requests:
			key := req.lock_name + "_" + req.entity
			if _, ok := conn.release_await[key]; !ok {
				//Key doesn't exist, send release request
				conn.release_await[key] = req.resp_chan
				conn.conn.Write([]byte("release_lock:"+req.lock_name+":"+req.entity+":\n"))
			} else {
				//Key already exists, trying to double release
				req.resp_chan <- false
			}

		}
	}
}

func runConnectionReader(conn *Connection){
	buf := make([]byte, 1024)
	for {

		_, err := conn.conn.Read(buf)
		if err != nil {
			log.Println("Error reading data:", err.Error())
			conn.conn.Close()
			return
		}

		//Find the terminal character in order to convert to string properly
		n := bytes.Index(buf, []byte{0})
		args := strings.Split(string(buf[:n]), ":")

		if args[0] == "lock_acquired" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.acquire_await[key]; ok {
				//Key exists, we can push to the channel
				conn.acquire_await[key] <- true
				delete(conn.acquire_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never requested. Lock:"+args[1] + ". Entity: "+args[2])
			}
		} else if args[0] == "lock_released" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.release_await[key]; ok {
				//Key exists, we can push to the channel
				conn.release_await[key] <- true
				delete(conn.release_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never requested. Lock:"+args[1] + ". Entity: "+args[2])
			}
		} else {
			log.Println("UNKNOWN COMMAND "+args[0])
		}


	}
}

func Connect(host string, port string) (*Connection, error) {
	conn, err := net.Dial("tcp", host+":"+port)

	_conn := Connection{conn, make(chan request), make(chan request),
		make(map[string]chan bool), make(map[string]chan bool)}
	if err != nil {
		return &_conn, err
	}

	go runConnectionWriter(&_conn)
	go runConnectionReader(&_conn)

	return &_conn, err
}

func AcquireLock(conn *Connection, lock_name string, entity string) bool{
	resp_chan := make(chan bool)

	//Queue lock request
	conn.acquire_requests <- request{lock_name, entity, resp_chan}

	//Wait for lock acquirement (or failure)
	b := <- resp_chan
	return b
}

func ReleaseLock(conn *Connection, lock_name string, entity string) bool{
	resp_chan := make(chan bool)

	//Queue release request
	conn.release_requests <- request{lock_name, entity, resp_chan}

	//Wait for lock release (or failure)
	b := <- resp_chan

	return b
}
