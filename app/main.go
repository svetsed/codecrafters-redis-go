package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type entry struct {
	value 	  any
	expiresAt int64
}

var (
	store = make(map[string]entry)
	mu sync.RWMutex
)

var (
	ErrReading = errors.New("read error")
	ErrInvalidRequest = errors.New("invalid request")
	ErrInvalidNum = errors.New("invalid number")
)

func main() {
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

func ReadNumber(reader *bufio.Reader) (int, error) {
	bt, err := reader.ReadBytes('\n')
	if err != nil {
		return  0, ErrReading
	}

	if len(bt) < 3 {
		return 0, ErrInvalidRequest
	}

	if bt[len(bt)-2] != '\r' {
		return 0, ErrInvalidRequest
	}

	n, err := strconv.Atoi(string(bt[:len(bt)-2]))
	if err != nil {
		return 0, err
	}

	return n, nil
}

func ReadArg(reader *bufio.Reader, numByte int) (string, error) {
	data := make([]byte, numByte)
	_, err := io.ReadFull(reader, data)
	if err != nil {
		return "", ErrReading
	}

	b1, err := reader.ReadByte()
	if err != nil {
		return "", ErrReading
	}
	b2, err := reader.ReadByte()
	if err != nil {
		return "", ErrReading
	}
	if b1 != '\r' || b2 != '\n' {
		return "", ErrInvalidRequest
	}

	return string(data), nil
}

func HandleArgs(conn net.Conn, cmdAndArgs []string) {
	if len(cmdAndArgs) == 0 {
		return
	}

	cmd := strings.ToLower(cmdAndArgs[0])
	args := cmdAndArgs[1:]

	fmt.Println("cmd=", cmd, "args=", args)
	switch cmd {
	case "echo":
		if len(args) == 1 {
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(args[0]), args[0])
		} else if len(args) == 0 {
			fmt.Fprint(conn, "$0\r\n\r\n")
		} else {
			resp := fmt.Sprintf("*%d\r\n", len(args))
			for _, msg := range args {
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
			}
			fmt.Fprint(conn, resp)
		}
	case "ping":
		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			fmt.Println("Error writing response on ping: ", err.Error())
			return
		}
	case "set":
		if len(args) < 2 { // min key value
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on set: ", err.Error())
			}
			return
		} else if len(args) == 2 {
			mu.Lock()
			store[args[0]] = entry{value: args[1], expiresAt: -1}
			mu.Unlock()
		} else if len(args) >= 4 { // key value [px] time
			if strings.ToLower(args[2]) == "px" {
				var expiresAt int64 = -1
				ms, err := strconv.Atoi(args[3])
				if err != nil {
					break
				}
				if ms > 0 {
					expiresAt = time.Now().UnixNano()/1e6 + int64(ms)
				}
				mu.Lock()
				store[args[0]] = entry{value: args[1], expiresAt:expiresAt}
				mu.Unlock()
			}
		}
		
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			fmt.Println("Error writing response 'OK' on set: ", err.Error())
			return
		}
	case "get": 
		if len(args) < 1 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on get: ", err.Error())
			}
			return
		}

		mu.RLock()
		val, ok := store[args[0]]
		mu.RUnlock()

		if !ok {
			if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
				fmt.Println("Error writing response 'no key' on get: ", err.Error())
			}
			return
		}
		
		if val.expiresAt != -1 && time.Now().UnixNano()/1e6 > val.expiresAt {
			mu.Lock()
			delete(store, args[0])
			mu.Unlock()
			if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
				fmt.Println("Error writing response 'no key' on get: ", err.Error())
			}
			return
		}

		v, ok := val.value.(string); 
		if !ok {
			_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			if err != nil {
				fmt.Println("Error writing response 'wrongtype' on get: ", err.Error())
			}
			return
		}

		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
	case "rpush":
		fmt.Println("RPUSH called with args:", args)
		if len(args) < 2 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on rpush: ", err.Error())
			}
			return
		}

		mu.RLock()
		val, exist := store[args[0]]
		mu.RUnlock()

		if exist {
			v, ok := val.value.([]string)
			if !ok {
				_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				if err != nil {
					fmt.Println("Error writing response 'wrongtype' on rpush: ", err.Error())
				}
				return
			}
			
			v = append(v, args[1:]...)
			mu.Lock()
			store[args[0]] = entry{value: v, expiresAt: val.expiresAt}
			mu.Unlock()

			fmt.Fprintf(conn, ":%d\r\n", len(v))
			return
		}

		values := []string{}
		values = append(values, args[1:]...)
		mu.Lock()
		store[args[0]] = entry{value: values, expiresAt: -1}
		mu.Unlock()

		fmt.Fprintf(conn, ":%d\r\n", len(values))
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		firstByte, err := reader.ReadByte()
		if err != nil {
			break
		}
		switch firstByte {
		case '*':
			N, err := ReadNumber(reader)
			if err != nil {
				break
			}
			
			if N <= 0 {
				break
			}

			args := make([]string, N)
			
			for range N {
				bt, err := reader.ReadByte()
				if err != nil {
					break
				}

				if bt == '$' {
					n, err := ReadNumber(reader)
					if err != nil {
						break
					}
					if n <= 0 {
						break
					}
					arg, err := ReadArg(reader, n)
					if err != nil {
						break
					}
					fmt.Println("arg=", arg)
					args = append(args, arg)
				}
			}

			HandleArgs(conn, args)
		case '$':
			n, err := ReadNumber(reader)
			if err != nil {
				break
			}
			if n <= 0 {
				break
			}
			arg, err := ReadArg(reader, n)
			if err != nil {
				break
			}

			// HandleArgs(conn, arg)
		default:
			reader.ReadBytes('\n')
			conn.Write([]byte("-ERR Protocol error: unexpected first byte\r\n"))
			return
		}
	}
}

// (printf '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n';) | nc localhost 6379