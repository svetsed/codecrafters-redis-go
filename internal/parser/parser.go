package parser

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/model"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

var (
	ErrReading        = errors.New("read error")
	ErrInvalidRequest = errors.New("invalid request")
	ErrInvalidNum     = errors.New("invalid number")
)

type Parser struct {
	storage *storage.Storage
}

func NewParser(storage *storage.Storage) *Parser {
	return &Parser{
		storage: storage,
	}
}

func (p *Parser) ReadNumber(reader *bufio.Reader) (int, error) {
	bt, err := reader.ReadBytes('\n')
	if err != nil {
		return 0, ErrReading
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

func (p *Parser) ReadArg(reader *bufio.Reader, numByte int) (string, error) {
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

func (p *Parser) HandleArgs(conn net.Conn, cmdAndArgs ...string) {
	if len(cmdAndArgs) == 0 {
		conn.Write([]byte("-ERR empty command\r\n"))
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
			p.storage.SetValue(args[0], model.Entry{Value: args[1], ExpiresAt: -1})
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
				p.storage.SetValue(args[0], model.Entry{Value: args[1], ExpiresAt: expiresAt})
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

		val, ok := p.storage.GetValue(args[0])

		if !ok {
			if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
				fmt.Println("Error writing response 'no key' on get: ", err.Error())
			}
			return
		}

		if val.ExpiresAt != -1 && time.Now().UnixNano()/1e6 > val.ExpiresAt {
			p.storage.DeleteValue(args[0])
			if _, err := conn.Write([]byte("$-1\r\n")); err != nil {
				fmt.Println("Error writing response 'no key' on get: ", err.Error())
			}
			return
		}

		v, ok := val.Value.(string)
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

		val, exist := p.storage.GetValue(args[0])
		if exist {
			v, ok := val.Value.([]string)
			if !ok {
				_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				if err != nil {
					fmt.Println("Error writing response 'wrongtype' on rpush: ", err.Error())
				}
				return
			}
			oldLenV := len(v)
			v = append(v, args[1:]...)
			ok = p.storage.UpdateOrSetValue(args[0], model.Entry{Value: v, ExpiresAt: val.ExpiresAt}, oldLenV)
			if !ok {
				if _, err := conn.Write([]byte("-ERR 'rpush' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on rpush: ", err.Error())
				}
				return
			}

			fmt.Fprintf(conn, ":%d\r\n", len(v))
			return
		}

		values := []string{}
		values = append(values, args[1:]...)
		ok := p.storage.UpdateOrSetValue(args[0], model.Entry{Value: values, ExpiresAt: -1}, 0)
		if !ok {
			if _, err := conn.Write([]byte("-ERR 'rpush' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on rpush: ", err.Error())
			}
			return
		}

		fmt.Fprintf(conn, ":%d\r\n", len(values))
	case "lrange":
		if len(args) < 3 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lrange: ", err.Error())
			}
			return
		}

		val, exist := p.storage.GetValue(args[0])
		if !exist {
			if _, err := conn.Write([]byte("*0\r\n")); err != nil {
				fmt.Println("Error writing response 'empty array' on lrange: ", err.Error())
			}
			return
		}

		start, err := strconv.Atoi(args[1])
		if err != nil {
			if _, err := conn.Write([]byte("-ERR not a numder index for 'lrange' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lrange: ", err.Error())
			}
			return
		}
		stop, err := strconv.Atoi(args[2])
		if err != nil {
			if _, err := conn.Write([]byte("-ERR not a numder index for 'lrange' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lrange: ", err.Error())
			}
			return
		}

		v, ok := val.Value.([]string)
		if !ok {
			_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			if err != nil {
				fmt.Println("Error writing response 'wrongtype' on lrange: ", err.Error())
			}
			return
		}
		
		if start >= len(v) {
			if _, err := conn.Write([]byte("*0\r\n")); err != nil {
				fmt.Println("Error writing response 'empty array' on lrange: ", err.Error())
			}
			return
		}
		if start > stop {
			if _, err := conn.Write([]byte("*0\r\n")); err != nil {
				fmt.Println("Error writing response 'empty array' on lrange: ", err.Error())
			}
			return
		}
		if stop > len(v) {
			stop = len(v)
		}

		respArr := v[start:stop]
		resp := fmt.Sprintf("*%d\r\n", len(respArr))
		for _, elem := range respArr {
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(elem), elem)
		}

		if _, err := conn.Write([]byte(resp)); err != nil {
			fmt.Println("Error writing response 'not empty array' on lrange: ", err.Error())
		}
		return
	}
}

func (p *Parser) HandleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		firstByte, err := reader.ReadByte()
		if err != nil {
			break
		}
		switch firstByte {
		case '*':
			N, err := p.ReadNumber(reader)
			if err != nil {
				break
			}

			if N <= 0 {
				break
			}

			args := make([]string, 0, N)

			for range N {
				bt, err := reader.ReadByte()
				if err != nil {
					break
				}

				if bt == '$' {
					n, err := p.ReadNumber(reader)
					if err != nil {
						break
					}
					if n <= 0 {
						break
					}
					arg, err := p.ReadArg(reader, n)
					if err != nil {
						break
					}
					fmt.Println("arg=", arg)
					args = append(args, arg)
				}
			}

			p.HandleArgs(conn, args...)
		case '$':
			n, err := p.ReadNumber(reader)
			if err != nil {
				break
			}
			if n <= 0 {
				break
			}
			arg, err := p.ReadArg(reader, n)
			if err != nil {
				break
			}

			p.HandleArgs(conn, arg)
		default:
			reader.ReadBytes('\n')
			conn.Write([]byte("-ERR Protocol error: unexpected first byte\r\n"))
			return
		}
	}
}
