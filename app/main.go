package main

import (
	"fmt"
	"net"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/codecrafters-io/redis-starter-go/internal/handler"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/storage"
	"github.com/codecrafters-io/redis-starter-go/internal/subscriber"
)

func pprof() {
	// Запускаем HTTP-сервер для pprof
    go func() {
        fmt.Println("pprof server listening on http://localhost:6060/debug/pprof/")
        if err := http.ListenAndServe("localhost:6060", nil); err != nil {
            fmt.Printf("pprof error: %v\n", err)
        }
    }()
}

func main() {
	_, exist := os.LookupEnv("RUN_PPROF_SERV")
	if exist {
		pprof()
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	subs := subscriber.NewSubscribers()
	storage := storage.NewStorage()
	handler := handler.NewHandler(storage, subs)
	parser := parser.NewParser(handler)

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
