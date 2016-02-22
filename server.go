package dlock

import (
	"strings"
	"fmt"
	"net"
	"os"
	//"bytes"
	"bufio"
)

func main(){
	RunServer("localhost", "8422")
}

func RunServer(host string, port string) {
	//Initialize dlock memory goodies
	state := Initialize()

	//Setup the connection
	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		fmt.Println("Dlock Error listening:", err.Error())
		os.Exit(1)
	}

	// Close listener when application closes
	defer listener.Close()

	// Listen for an incoming connection.
    fmt.Println("Dlock Listening on " + host + ":" + port)
    for {
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Error accepting TCP connection: ", err.Error())
            os.Exit(1)
        }
        // Handle connection
			go handleRequest(state, conn)
    }
}

// Handle Request
func handleRequest(state dlock_state, conn net.Conn) {
	buf := bufio.NewReader(conn)
	for {
		//Read data into buffer
		message, err := buf.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading data:", err.Error())
			conn.Close()
			return
		}

		args := strings.Split(message, "||")
		command := args[0]

		if command == "acquire_lock" {
			if len(args) < 3 {
				conn.Write([]byte("too_few_arguments\n"))
				continue;
			}

			lockName := args[1]
			entityName := args[2]
			go func(){
				AcquireLock(state, lockName, /*conn.RemoteAddr().String() +*/ "_" + entityName)
				conn.Write([]byte("lock_acquired||"+lockName+"||"+entityName+"||lock_acquired||\n"))
			}()
		} else if command == "try_acquire_lock" {
			if len(args) < 3 {
				conn.Write([]byte("too_few_arguments\n"))
				continue;
			}

			lockName := args[1]
			entityName := args[2]
			s := TryAcquireLock(state, lockName, /*conn.RemoteAddr().String() +*/ "_" + entityName)
			if "success" == s {

				conn.Write([]byte("lock_acquired||"+lockName+"||"+entityName+"||lock_acquired||\n"))
			} else {
				conn.Write([]byte("lock_try_acquire_failed||"+lockName+"||"+entityName+"||"+s+"||\n"))				}
		} else if command == "release_lock" {
			if len(args) < 3 {
				conn.Write([]byte("too_few_arguments\n"))
				continue;
			}
			lockName := args[1]
			entityName := args[2]

			go func(){
				res := ReleaseLock(state, lockName, /*conn.RemoteAddr().String() + */"_" + entityName)
				if res == "lock_released" {
					conn.Write([]byte("lock_released||"+lockName+"||"+entityName+"||lock_released||\n"))
				} else {
					conn.Write([]byte("lock_released||"+lockName+"||"+entityName+"||release_error||\n"))
				}
			}()
		} else {

		}

		if strings.TrimSpace(command) == "close" {
			conn.Close()
			return
		}
	}
}
