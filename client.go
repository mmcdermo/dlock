package dlock

import (
	"bytes"
	"strings"
	"net"
	"log"
)

type Connection struct {
	conn net.Conn
	acquire_requests chan request
	try_acquire_requests chan request
	release_requests chan request
	acquire_await map[string]chan string
	release_await map[string]chan string
}

func runConnectionWriter(conn *Connection){
	for {
		select {
		case req := <- conn.try_acquire_requests:
			key := req.name + "_" + req.owner
			if _, ok := conn.acquire_await[key]; !ok {
				//Key doesn't exist, send lock request
				conn.acquire_await[key] = req.resp_chan
				conn.conn.Write([]byte("try_acquire_lock:"+req.name+":"+req.owner+":\n"))
			} else {
				//Key already exists, trying to double lock
				req.resp_chan <- "error:Double lock"
			}
		case req := <- conn.acquire_requests:
			key := req.name + "_" + req.owner
			if _, ok := conn.acquire_await[key]; !ok {
				//Key doesn't exist, send lock request
				conn.acquire_await[key] = req.resp_chan

				conn.conn.Write([]byte("acquire_lock:"+req.name+":"+req.owner+":\n"))
			} else {
				//Key already exists, trying to double lock
				req.resp_chan <- "error:Double lock"
			}

		case req := <- conn.release_requests:
			key := req.name + "_" + req.owner
			if _, ok := conn.release_await[key]; !ok {
				//Key doesn't exist, send release request
				conn.release_await[key] = req.resp_chan
				conn.conn.Write([]byte("release_lock:"+req.name+":"+req.owner+":\n"))
			} else {
				//Key already exists, trying to double release
				req.resp_chan <- "error:Double lock"
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
				conn.acquire_await[key] <- args[3]
				delete(conn.acquire_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never requested. Lock:"+args[1] + ". Entity: "+args[2])
			}
		} else if args[0] == "lock_try_acquire_failed" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.acquire_await[key]; ok {
				//Key exists, we can push to the channel
				conn.acquire_await[key] <- args[3]
				delete(conn.acquire_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never requested. Lock:"+args[1] + ". Entity: "+args[2])
			}

		} else if args[0] == "lock_released" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.release_await[key]; ok {
				//Key exists, we can push to the channel
				conn.release_await[key] <- args[3]
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

	_conn := Connection{conn,
		make(chan request),
		make(chan request),
		make(chan request),
		make(map[string]chan string),
		make(map[string]chan string),
	}
	if err != nil {
		return &_conn, err
	}

	go runConnectionWriter(&_conn)
	go runConnectionReader(&_conn)

	return &_conn, err
}

func ClientAcquireLock(conn *Connection, lock_name string, entity string) string {
	resp_chan := make(chan string)

	//Queue lock request
	conn.acquire_requests <- request{lock_name, entity, resp_chan}

	//Wait for lock acquirement
	s:= <- resp_chan
	return s
}

func ClientTryAcquireLock(conn *Connection, lock_name string, entity string) string {
	resp_chan := make(chan string)

	//Queue lock request
	conn.try_acquire_requests <- request{lock_name, entity, resp_chan}

	//Wait for lock acquirement (or failure)
	s:= <- resp_chan
	return s
}


func ClientReleaseLock(conn *Connection, lock_name string, entity string) string {
	resp_chan := make(chan string)

	//Queue release request
	conn.release_requests <- request{lock_name, entity, resp_chan}

	//Wait for lock release (or failure)
	s:= <- resp_chan

	return s
}
