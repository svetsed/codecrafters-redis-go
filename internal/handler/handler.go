package handler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/model"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
	"github.com/codecrafters-io/redis-starter-go/internal/subscriber"
	"github.com/codecrafters-io/redis-starter-go/internal/writer"
)

type Handler struct {
	storage *storage.Storage
	subs    *subscriber.Subscribers
}

func NewHandler(storage *storage.Storage, subs *subscriber.Subscribers) *Handler {
	return &Handler{
		storage: storage,
		subs:    subs,
	}
}

func (h *Handler) HandleArgs(client *model.Client, cmdAndArgs ...string) {
	conn := client.Conn
	if len(cmdAndArgs) == 0 {
		conn.Write([]byte("-ERR empty command\r\n"))
		return
	}

	cmd := strings.ToLower(cmdAndArgs[0])
	args := cmdAndArgs[1:]

	switch cmd {
	case "echo":
		if len(args) == 0 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on echo: ", err.Error())
			}
			return
		}

		if len(args) == 1 {
			if err := writer.WriteBulkString(conn, args[0]); err != nil {
				fmt.Println("Error writing response 'bulk string' on echo: ", err.Error())
			}
			return
		}

		if err := writer.WriteRESPArray(conn, args); err != nil {
			fmt.Println("Error writing response 'RESP array' on echo: ", err.Error())
		}
		
	case "ping":
		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			fmt.Println("Error writing response on ping: ", err.Error())
		}

	case "set":
		if len(args) < 2 || len(args) == 3 { // min key value
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on set: ", err.Error())
			}
			return
		} 
		
		if len(args) == 2 {
			h.storage.SetValue(args[0], model.Entry{Value: args[1], ExpiresAt: -1})
		} else if len(args) >= 4 { // key value [px] time
			if strings.ToLower(args[2]) == "px" {
				var expiresAt int64 = -1
				ms, err := strconv.Atoi(args[3])
				if err != nil {
					if _, err := conn.Write([]byte("-ERR arg after [px] must be a number for 'set' command\r\n")); err != nil {
						fmt.Println("Error writing response 'ERR' on set: ", err.Error())
					}
					return
				}
				if ms > 0 {
					expiresAt = time.Now().UnixNano()/1e6 + int64(ms)
				}
				h.storage.SetValue(args[0], model.Entry{Value: args[1], ExpiresAt: expiresAt})
			}
		}

		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			fmt.Println("Error writing response 'OK' on set: ", err.Error())
		}

	case "get":
		if len(args) < 1 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on get: ", err.Error())
			}
			return
		}

		val, ok := h.storage.GetValue(args[0])
		if !ok {
			if _, err := conn.Write([]byte(writer.NullBulkString)); err != nil {
				fmt.Println("Error writing response 'no key' on get: ", err.Error())
			}
			return
		}

		if val.ExpiresAt != -1 && time.Now().UnixNano()/1e6 > val.ExpiresAt {
			h.storage.DeleteValue(args[0])
			if _, err := conn.Write([]byte(writer.NullBulkString)); err != nil {
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

		if err := writer.WriteBulkString(conn, v); err != nil {
			fmt.Println("Error writing response 'bulk string' on get: ", err.Error())
		}

	case "rpush":
		if len(args) < 2 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on rpush: ", err.Error())
			}
			return
		}

		newLen, err := h.storage.RPush(args[0], args[1:])
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				if err != nil {
					fmt.Println("Error writing response 'wrongtype' on rpush: ", err.Error())
				}
				return
			}

			if _, err := conn.Write([]byte("-ERR 'rpush' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on rpush: ", err.Error())
			}
			return
		}

		fmt.Fprintf(conn, ":%d\r\n", newLen)

		//blpop
		if client, ok := h.subs.Get(args[0]); ok {
			removed, err := h.storage.LPopFirst(args[0])
			if err != nil {
				if errors.Is(err, storage.NoValues) {
					h.subs.Append(client, args[0])
					return
				}

				if errors.Is(err, storage.WrongType) {
					_, err := client.Conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
					if err != nil {
						fmt.Println("Error writing response 'wrongtype' on blpop (deferred in rpush): ", err.Error())
					}
					return
				}

				if _, err := client.Conn.Write([]byte("-ERR 'blpop' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on blpop (deferred in rpush): ", err.Error())
				}
				return
			}

			client.WakeUpChan <- &model.WakeUpData{Key: args[0], Value: removed}
		}

	case "lrange":
		if len(args) < 3 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lrange: ", err.Error())
			}
			return
		}

		val, exist := h.storage.GetValue(args[0])
		if !exist {
			if _, err := conn.Write([]byte(writer.EmptyArray)); err != nil {
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

		if start < 0 {
			start = len(v)+start
			if start < 0 {
				start = 0
			}
		}

		if stop < 0 {
			stop = len(v)+stop
			if stop < 0 {
				stop = 0
			}
		}

		if start >= len(v) {
			if _, err := conn.Write([]byte(writer.EmptyArray)); err != nil {
				fmt.Println("Error writing response 'empty array' on lrange: ", err.Error())
			}
			return
		}
		if start > stop {
			if _, err := conn.Write([]byte(writer.EmptyArray)); err != nil {
				fmt.Println("Error writing response 'empty array' on lrange: ", err.Error())
			}
			return
		}
		if stop >= len(v) {
			stop = len(v)-1
		}

		err = writer.WriteRESPArray(conn, v[start:stop+1]) //inclusive
		if err != nil {
			fmt.Println("Error writing response 'RESP array' on lrange: ", err.Error())
		}

	case "lpush":
		if len(args) < 2 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lpush: ", err.Error())
			}
			return
		}

		// invert
		newVals := make([]string, 0, len(args[1:]))
		for i := len(args)-1; i >= 1; i-- {
			newVals = append(newVals, args[i])
		}

		newLen, err := h.storage.LPush(args[0], newVals)
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				if err != nil {
					fmt.Println("Error writing response 'wrongtype' on lpush: ", err.Error())
				}
				return
			}

			if _, err := conn.Write([]byte("-ERR 'lpush' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lpush: ", err.Error())
			}
			return
		}

		fmt.Fprintf(conn, ":%d\r\n", newLen)

		//blpop
		if client, ok := h.subs.Get(args[0]); ok {
			removed, err := h.storage.LPopFirst(args[0])
			if err != nil {
				if errors.Is(err, storage.NoValues) {
					h.subs.Append(client, args[0])
					return
				}

				if errors.Is(err, storage.WrongType) {
					_, err := client.Conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
					if err != nil {
						fmt.Println("Error writing response 'wrongtype' on blpop (deferred in lpush): ", err.Error())
					}
					return
				}

				if _, err := client.Conn.Write([]byte("-ERR 'blpop' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on blpop (deferred in lpush): ", err.Error())
				}
				return
			}

			client.WakeUpChan <- &model.WakeUpData{Key: args[0], Value: removed}
		}

	case "llen":
		if len(args) != 1 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on llen: ", err.Error())
			}
			return
		}

		length, ok := h.storage.GetLen(args[0])
		if !ok {
			if _, err := conn.Write([]byte(":0\r\n")); err != nil {
				fmt.Println("Error writing response ':0' on llen: ", err.Error())
			}
			return
		}

		fmt.Fprintf(conn, ":%d\r\n", length)

	case "lpop":
		if len(args) < 1 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lpop: ", err.Error())
			}
			return
		}

		n := 0
		if len(args) == 1 {
			n = 1
		} else {
			tmp, err  := strconv.Atoi(args[1])
			if err != nil {
				if _, err := conn.Write([]byte("-ERR failed convert to number argument 'lpop' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on lpop: ", err.Error())
				}
				return
			}
			n = tmp
		}

		removed, err := h.storage.LPop(args[0], n)
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				if err != nil {
					fmt.Println("Error writing response 'wrongtype' on lpop: ", err.Error())
				}
				return
			}
			if _, err := conn.Write([]byte("-ERR failed to delete in 'lpop' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on lpop: ", err.Error())
			}
			return
		}

		if removed == nil {
			if _, err := conn.Write([]byte(writer.NullBulkString)); err != nil {
				fmt.Println("Error writing response 'null bulk string' on lpop: ", err.Error())
			}
			return
		}

		if len(removed) == 1 {
			if err := writer.WriteBulkString(conn, removed[0]); err != nil {
				fmt.Println("Error writing response 'bulk string' on lpop: ", err.Error())
			}
			return
		}

		if err := writer.WriteRESPArray(conn, removed); err != nil {
			fmt.Println("Error writing response 'RESP array' on lpop: ", err.Error())
		}

	case "blpop":
		if len(args) < 2 {
			if _, err := conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on blpop: ", err.Error())
			}
			return
		}

		timeoutSec, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			if _, err := conn.Write([]byte("-ERR arg after [key] must be a number for 'blpop' command\r\n")); err != nil {
				fmt.Println("Error writing response 'ERR' on blpop: ", err.Error())
			}
			return
		}

		removed, err := h.storage.LPopFirst(args[0])
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				_, err := conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
				if err != nil {
					fmt.Println("Error writing response 'wrongtype' on blpop: ", err.Error())
				}
				return
			}

			if !errors.Is(err, storage.NoValues) {
				if _, err := conn.Write([]byte("-ERR failed to get value in 'blpop' command\r\n")); err != nil {
					fmt.Println("Error writing response 'ERR' on blpop: ", err.Error())
				}
				return
			}
		}

		if removed != "" {
			if err := writer.WriteRESPArray(conn, []string{args[0], removed}); err != nil {
				fmt.Println("Error writing response 'RESP array' on blpop: ", err.Error())
			}
			return
		}

		h.subs.Append(client, args[0])


		if timeoutSec <= 0 {
			res :=  <- client.WakeUpChan
			if err := writer.WriteRESPArray(client.Conn, []string{res.Key, res.Value}); err != nil {
				fmt.Println("Error writing response 'RESP array' on blpop deferred: ", err.Error())
			}
		} else {
			timeoutDur := time.Duration(timeoutSec * float64(time.Second))
			select {
			case res :=  <- client.WakeUpChan:
				if err := writer.WriteRESPArray(client.Conn, []string{res.Key, res.Value}); err != nil {
					fmt.Println("Error writing response 'RESP array' on blpop deferred: ", err.Error())
				}
			case <- time.After(timeoutDur):
				if _, err := client.Conn.Write([]byte(writer.EmptyArray)); err != nil {
					fmt.Println("Error writing response 'null array timeout' on blpop deferred: ", err.Error())
				}
				if ok := h.subs.RemoveClient(client, args[0]); !ok {
					fmt.Println("Error remove client from queue on blpop deferred")
				}
			default:
			}
		}
		
	}
}