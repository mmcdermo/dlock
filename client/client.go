package dlock

import (
	"bytes"
	"strings"
	"net"
	"log"
	"github.com/mmcdermo/dlock"
)

func (c *dlock.Connection) Close() error {
	return c.conn.Close()
}

func runConnectionWriter(conn *dlock.Connection){
	for {
		select {
		case req := <- conn.try_acquire_dlock.requests:
			key := req.name + "_" + req.owner
			if _, ok := conn.acquire_await[key]; !ok {
				//Key doesn't exist, send lock dlock.request
				conn.acquire_await[key] = req.resp_chan
				conn.conn.Write([]byte("try_acquire_lock||"+req.name+"||"+req.owner+"||\n"))
			} else {
				//Key already exists, trying to double lock
				req.resp_chan <- "Error: Double try acquire"
			}
		case req := <- conn.acquire_dlock.requests:
			key := req.name + "_" + req.owner
			if _, ok := conn.acquire_await[key]; !ok {
				//Key doesn't exist, send lock dlock.request
				conn.acquire_await[key] = req.resp_chan

				conn.conn.Write([]byte("acquire_lock||"+req.name+"||"+req.owner+"||\n"))
			} else {
				//Key already exists, trying to double lock
				req.resp_chan <- "Error: Double acquire"
			}

		case req := <- conn.release_dlock.requests:
			key := req.name + "_" + req.owner
			if _, ok := conn.release_await[key]; !ok {
				//Key doesn't exist, send release dlock.request
				conn.release_await[key] = req.resp_chan
				conn.conn.Write([]byte("release_lock||"+req.name+"||"+req.owner+"||\n"))
			} else {
				//Key already exists, trying to double release
				req.resp_chan <- "Eerror: Double release"
			}

		}
	}
}

func runConnectionReader(conn *dlock.Connection){
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
		args := strings.Split(string(buf[:n]), "||")

		if args[0] == "lock_acquired" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.acquire_await[key]; ok {
				//Key exists, we can push to the channel
				conn.acquire_await[key] <- args[3]
				delete(conn.acquire_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never dlock.requested. Lock:"+args[1] + ". Entity: "+args[2])
			}
		} else if args[0] == "lock_try_acquire_failed" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.acquire_await[key]; ok {
				//Key exists, we can push to the channel
				conn.acquire_await[key] <- args[3]
				delete(conn.acquire_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never dlock.requested. Lock:"+args[1] + ". Entity: "+args[2])
			}

		} else if args[0] == "lock_released" && len(args) >= 3 {
			key := args[1] + "_" + args[2]
			if _, ok := conn.release_await[key]; ok {
				//Key exists, we can push to the channel
				conn.release_await[key] <- args[3]
				delete(conn.release_await, key)
			} else {
				log.Println("Error: Lock/Entity combination never dlock.requested. Lock:"+args[1] + ". Entity: "+args[2])
			}
		} else {
			log.Println("UNKNOWN COMMAND "+args[0])
		}


	}
}

func Connect(host string, port string) (*dlock.Connection, error) {
	conn, err := net.Dial("tcp", host+":"+port)

	_conn := dlock.Connection{conn,
		make(chan dlock.request),
		make(chan dlock.request),
		make(chan dlock.request),
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

func ClientAcquireLock(conn *dlock.Connection, lock_name string, entity string) string {
	resp_chan := make(chan string)

	//Queue lock dlock.request
	conn.acquire_dlock.requests <- dlock.request{lock_name, entity, resp_chan}

	//Wait for lock acquirement
	s:= <- resp_chan
	return s
}

func ClientTryAcquireLock(conn *dlock.Connection, lock_name string, entity string) string {
	resp_chan := make(chan string)

	//Queue lock dlock.request
	conn.try_acquire_dlock.requests <- dlock.request{lock_name, entity, resp_chan}

	//Wait for lock acquirement (or failure)
	s:= <- resp_chan
	return s
}


func ClientReleaseLock(conn *dlock.Connection, lock_name string, entity string) (bool, string) {
	resp_chan := make(chan string)

	//Queue release dlock.request
	conn.release_dlock.requests <- dlock.request{lock_name, entity, resp_chan}

	//Wait for lock release (or failure)
	s:= <- resp_chan

	return s == "lock_released", s
}
