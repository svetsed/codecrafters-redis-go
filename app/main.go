package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type entry struct {
	value 	  string
	expiresAt int64
}

var (
	store = make(map[string]entry)
	mute sync.RWMutex
)
func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			return
		}

		str := string(buf)
		sliceCom := strings.Split(str, "\r\n")

		switch strings.ToLower(sliceCom[2]) {
		case "echo":
			mess := sliceCom[4]
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(mess), mess)
		case "ping":
			if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
				fmt.Println("Error writing response on ping: ", err.Error())
				return
			}
		case "set":
			if len(sliceCom) < 7 {
				if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on set: ", err.Error())
				}
				return
			} else if len(sliceCom) == 8 {
				mute.Lock()
				store[sliceCom[4]] = entry{value: sliceCom[6], expiresAt: -1}
				mute.Unlock()
			} else if len(sliceCom) == 12 {
				if strings.ToLower(sliceCom[8]) == "px" {
					var expiresAt int64 = -1
					ms, _ := strconv.Atoi(sliceCom[10])
					if ms > 0 {
						expiresAt = time.Now().UnixNano()/1e6 + int64(ms)
					}
					mute.Lock()
					store[sliceCom[4]] = entry{value: sliceCom[6], expiresAt:expiresAt}
					mute.Unlock()
				}
			}
			
			if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
				fmt.Println("Error writing response 'OK' on set: ", err.Error())
				return
			}
		case "get":
			if len(sliceCom) < 5 {
				if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on get: ", err.Error())
				}
				return
			}

			mute.RLock()
			val, ok := store[sliceCom[4]]
			mute.RUnlock()

			if !ok {
				if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
					fmt.Println("Error writing response 'no key' on get: ", err.Error())
				}
				return
			}
			
			if val.expiresAt != -1 && time.Now().UnixNano()/1e6 > val.expiresAt {
				mute.Lock()
				delete(store, sliceCom[4])
				mute.Unlock()
				if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
					fmt.Println("Error writing response 'no key' on get: ", err.Error())
				}
				return
			} 
				
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val.value), val.value)

		}
	}
}
