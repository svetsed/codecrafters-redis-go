package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	storage := storage.NewStorage()
	parser := parser.NewParser(storage)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go parser.HandleConn(conn)
	}
}

// example
// (printf '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n';) | nc localhost 6379