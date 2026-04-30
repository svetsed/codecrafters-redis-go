package writer

import (
	"bytes"
	"net"
	"strconv"
	"sync"
)

const (
	EmptyArray 	   = "*0\r\n"
	NullArray      = "*-1\r\n"
	NullBulkString = "$-1\r\n"
)

var bufPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

func WriteRESPArray(conn net.Conn, items []string) error {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(len(items)))
	buf.WriteString("\r\n")

	for _, s := range items {
        buf.WriteString("$")
        buf.WriteString(strconv.Itoa(len(s)))
        buf.WriteString("\r\n")
        buf.WriteString(s)
        buf.WriteString("\r\n")
    }

	_, err := conn.Write(buf.Bytes())
	return err
}

func WriteBulkString(conn net.Conn, item string) error {
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	buf.WriteString("$")
	buf.WriteString(strconv.Itoa(len(item)))
	buf.WriteString("\r\n")
	buf.WriteString(item)
	buf.WriteString("\r\n")

	_, err := conn.Write(buf.Bytes())
	return err
}