package handler

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/model"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
	"github.com/codecrafters-io/redis-starter-go/internal/subscriber"
	"github.com/codecrafters-io/redis-starter-go/internal/writer"
)

const (
	WRONGTYPE = "WRONGTYPE"
	ERR       = "ERR"
)

type Handler struct {
	storage *storage.Storage
	Subs    *subscriber.Subscribers
}

func NewHandler(storage *storage.Storage, subs *subscriber.Subscribers) *Handler {
	return &Handler{
		storage: storage,
		Subs:    subs,
	}
}

func (h *Handler) HandleArgs(client *model.Client, cmdAndArgs ...string) {
	conn := client.Conn
	if len(cmdAndArgs) == 0 {
		h.writeError(conn, "HandleArgs", ERR, "empty command")
		return
	}

	cmd := strings.ToLower(cmdAndArgs[0])
	args := cmdAndArgs[1:]

	switch cmd {
	case "echo":
		if len(args) == 0 {
			h.writeError(conn, "echo", ERR, "wrong number of arguments for 'echo' command")
			return
		}

		if len(args) == 1 {
			h.writeBulkString(conn, "echo", args[0])
			return
		}

		h.writeRESPArray(conn, "echo", args)

	case "ping":
		h.writeSimpleString(conn, "ping", "PONG")

	case "set":
		if len(args) < 2 { // min key value
			h.writeError(conn, "set", ERR, "wrong number of arguments for 'set' command")
			return
		}

		value := args[1]
		var expiresAt int64 = -1
		if len(args) > 2 {
			i := 2
			opt := strings.ToLower(args[i])
			if i+1 >= len(args) {
				h.writeError(conn, "set", ERR, "missing value for option")
				return
			}
			valStr := args[i+1]

			switch opt {
			case "px":
				// milliseconds
				ms, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					h.writeError(conn, "set", ERR, "argument after [px] must be a number")
					return
				}
				if ms > 0 {
					expiresAt = time.Now().UnixNano()/1e6 + int64(ms)
				}
			case "ex":
				// seconds
				sec, err := strconv.ParseFloat(valStr, 64)
				if err != nil {
					h.writeError(conn, "set", ERR, "argument after [ex] must be a number")
					return
				}
				if sec > 0 {
					expiresAt = time.Now().UnixNano()/1e6 + int64(sec*1000)
				}
			default:
				h.writeError(conn, "set", ERR, "unknown options for command")
				return
			}
		}

		h.storage.SetValue(args[0], model.Entry{Value: value, ExpiresAt: expiresAt})
		h.writeSimpleString(conn, "set", "OK")

	case "get":
		if len(args) < 1 {
			h.writeError(conn, "get", ERR, "wrong number of arguments for 'get' command")
			return
		}

		val, ok := h.storage.GetValue(args[0])
		if !ok {
			h.writeNullOrEmpty(conn, "get: no key", '$')
			return
		}

		if val.ExpiresAt != -1 && time.Now().UnixNano()/1e6 > val.ExpiresAt {
			h.storage.DeleteValue(args[0])
			h.writeNullOrEmpty(conn, "get: after DeleteValue", '$')
			return
		}

		v, ok := val.Value.(string)
		if !ok {
			h.writeError(conn, "get", WRONGTYPE, "operation against a key holding the wrong kind of value")
			return
		}

		h.writeBulkString(conn, "get", v)

	case "rpush":
		if len(args) < 2 {
			h.writeError(conn, "rpush", ERR, "wrong number of arguments for 'rpush' command")
			return
		}

		newLen, err := h.storage.RPush(args[0], args[1:])
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				h.writeError(conn, "rpush", WRONGTYPE, "operation against a key holding the wrong kind of value")
				return
			}
			h.writeError(conn, "rpush", ERR, "failed 'rpush' command")
			return
		}

		h.writeInteger(conn, "rpush", newLen)

		//blpop
		if client, ok := h.Subs.Get(args[0]); ok {
			removed, err := h.storage.LPopFirst(args[0])
			if err != nil {
				if errors.Is(err, storage.NoValues) {
					h.Subs.Append(client, args[0])
					return
				}
				if errors.Is(err, storage.WrongType) {
					h.writeError(client.Conn, "rpush-blpop", WRONGTYPE, "operation against a key holding the wrong kind of value")
					return
				}
				h.writeError(client.Conn, "rpush-blpop", ERR, "failed 'blpop' command")
				return
			}

			client.WakeUpChan <- &model.WakeUpData{Key: args[0], Value: removed}
		}

	case "lrange":
		if len(args) < 3 {
			h.writeError(conn, "lrange", ERR, "wrong number of arguments for 'lrange' command")
			return
		}

		val, exist := h.storage.GetValue(args[0])
		if !exist {
			h.writeNullOrEmpty(conn, "lrange: GetValue", '0')
			return
		}

		start, err := strconv.Atoi(args[1])
		if err != nil {
			h.writeError(conn, "lrange", ERR, "start index must be a number for 'lrange' command")
			return
		}

		stop, err := strconv.Atoi(args[2])
		if err != nil {
			h.writeError(conn, "lrange", ERR, "stop index must be a number for 'lrange' command")
			return
		}

		v, ok := val.Value.([]string)
		if !ok {
			h.writeError(conn, "lrange", WRONGTYPE, "operation against a key holding the wrong kind of value")
			return
		}

		normalizeIndex(&start, len(v))
		normalizeIndex(&stop, len(v))

		if start >= len(v) {
			h.writeNullOrEmpty(conn, "lrange: start > len(v)", '0')
			return
		}
		if start > stop {
			h.writeNullOrEmpty(conn, "lrange: start > stop", '0')
			return
		}
		if stop >= len(v) {
			stop = len(v) - 1
		}

		h.writeRESPArray(conn, "lrange", v[start:stop+1]) //inclusive

	case "lpush":
		if len(args) < 2 {
			h.writeError(conn, "lpush", ERR, "wrong number of arguments for 'lpush' command")
			return
		}

		// invert
		newVals := make([]string, 0, len(args[1:]))
		for i := len(args) - 1; i >= 1; i-- {
			newVals = append(newVals, args[i])
		}

		newLen, err := h.storage.LPush(args[0], newVals)
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				h.writeError(conn, "lpush", WRONGTYPE, "operation against a key holding the wrong kind of value")
				return
			}
			h.writeError(conn, "lpush", ERR, "failed 'lpush' command")
			return
		}

		h.writeInteger(conn, "lpush", newLen)

		//blpop
		if client, ok := h.Subs.Get(args[0]); ok {
			removed, err := h.storage.LPopFirst(args[0])
			if err != nil {
				if errors.Is(err, storage.NoValues) {
					// залогировать
					h.Subs.Append(client, args[0])
					return
				}
				if errors.Is(err, storage.WrongType) {
					h.writeError(client.Conn, "lpush-blpop", WRONGTYPE, "operation against a key holding the wrong kind of value")
					return
				}
				h.writeError(client.Conn, "lpush-blpop", ERR, "failed 'blpop command'")
				return
			}

			client.WakeUpChan <- &model.WakeUpData{Key: args[0], Value: removed}
		}

	case "llen":
		if len(args) != 1 {
			h.writeError(conn, "llen", ERR, "wrong number of arguments for 'llen' command")
			return
		}

		length, ok := h.storage.GetLen(args[0])
		if !ok {
			h.writeInteger(conn, "llen-zero", 0)
			return
		}

		h.writeInteger(conn, "llen", length)

	case "lpop":
		if len(args) < 1 {
			h.writeError(conn, "lpop", ERR, "wrong number of arguments for 'lpop' command")
			return
		}

		n := 0
		if len(args) == 1 {
			n = 1
		} else {
			tmp, err := strconv.Atoi(args[1])
			if err != nil {
				h.writeError(conn, "lpop", ERR, "failed convert argument to number for 'lpop' command")
				return
			}
			n = tmp
		}

		removed, err := h.storage.LPop(args[0], n)
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				h.writeError(conn, "lpop", WRONGTYPE, "operation against a key holding the wrong kind of value")
				return
			}
			h.writeError(conn, "lpop", ERR, "failed to delete in 'lpop' command")
			return
		}

		if removed == nil {
			h.writeNullOrEmpty(conn, "lpop", '$')
			return
		}

		if len(removed) == 1 {
			h.writeBulkString(conn, "lpop", removed[0])
			return
		}

		h.writeRESPArray(conn, "lpop", removed)

	case "blpop":
		if len(args) < 2 {
			h.writeError(conn, "blpop", ERR, "wrong number of arguments for 'blpop' command")
			return
		}

		timeoutSec, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			h.writeError(conn, "blpop", ERR, "argument after key must be a number for 'blpop' command")
			return
		}

		removed, err := h.storage.LPopFirst(args[0])
		if err != nil {
			if errors.Is(err, storage.WrongType) {
				h.writeError(conn, "blpop", WRONGTYPE, "operation against a key holding the wrong kind of value")
				return
			}

			if !errors.Is(err, storage.NoValues) {
				h.writeError(conn, "blpop", ERR, "failed to get value in 'blpop' command")
				return
			}
		}

		if removed != "" {
			h.writeRESPArray(conn, "blpop", []string{args[0], removed})
			return
		}

		// rpush/lpush may added something

		h.Subs.Append(client, args[0])

		if timeoutSec <= 0 {
			res := <-client.WakeUpChan
			h.writeRESPArray(client.Conn, "blpop", []string{res.Key, res.Value})
			return
		}

		timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
		select {
		case res := <-client.WakeUpChan:
			timer.Stop()
			h.writeRESPArray(client.Conn, "blpop", []string{res.Key, res.Value})
		case <-timer.C:
			h.writeNullOrEmpty(client.Conn, "blpop timeout", '*')
			if ok := h.Subs.RemoveClient(client, args[0]); !ok {
				fmt.Println("failed to remove client from queue on blpop deferred")
			}
		}

	}
}

// -ERR msg\r\n -WRONGTYPE msg\r\n
func (h *Handler) writeError(conn net.Conn, from string, code string, msg string) {
	switch code {
	case ERR:
		if _, err := conn.Write([]byte("-ERR " + msg + "\r\n")); err != nil {
			fmt.Printf("failed to write some error from '%s': %v\n", from, err)
		}
	case WRONGTYPE:
		if _, err := conn.Write([]byte("-WRONGTYPE " + msg + "\r\n")); err != nil {
			fmt.Printf("failed to write error wrongtype from '%s': %v\n", from, err)
		}
	default:
		fmt.Printf("WARN failed to write some error: unknown type '%s' from '%s'\n", code, from)
	}
}

// +msg\r\n
func (h *Handler) writeSimpleString(conn net.Conn, from string, msg string) {
	if _, err := conn.Write([]byte("+" + msg + "\r\n")); err != nil {
		fmt.Printf("failed to write simple string from '%s': %v\n", from, err)
	}
}

// :num\r\n
func (h *Handler) writeInteger(conn net.Conn, from string, num int) {
	if _, err := fmt.Fprintf(conn, ":%d\r\n", num); err != nil {
		fmt.Printf("failed to write RESP integer from '%s': %v\n", from, err)
	}
}

// null string $-1\r\n or null array *-1\r\n or empty *0\r\n
func (h *Handler) writeNullOrEmpty(conn net.Conn, from string, typeResp byte) {
	switch typeResp {
	case '$':
		if _, err := conn.Write([]byte(writer.NullBulkString)); err != nil {
			fmt.Printf("failed to write null bulk string from '%s': %v\n", from, err)
		}
	case '*':
		if _, err := conn.Write([]byte(writer.NullArray)); err != nil {
			fmt.Printf("failed to write null array from '%s': %v\n", from, err)
		}
	case '0':
		if _, err := conn.Write([]byte(writer.EmptyArray)); err != nil {
			fmt.Printf("failed to write empty array from '%s': %v\n", from, err)
		}
	default:
		fmt.Printf("WARN failed to write null string or array: unknown type '%c' from '%s'\n", typeResp, from)
	}
}

func (h *Handler) writeBulkString(conn net.Conn, from string, item string) {
	if err := writer.WriteBulkString(conn, item); err != nil {
		fmt.Printf("failed to write bulk string from '%s': %v\n", from, err)
	}
}

func (h *Handler) writeRESPArray(conn net.Conn, from string, items []string) {
	if err := writer.WriteRESPArray(conn, items); err != nil {
		fmt.Printf("failed to write RESP array from '%s': %v\n", from, err)
	}
}

func normalizeIndex(idx *int, length int) {
	if *idx < 0 {
		*idx += length
		if *idx < 0 {
			*idx = 0
		}
	}
}
