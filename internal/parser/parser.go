package parser

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/handler"
	"github.com/codecrafters-io/redis-starter-go/internal/model"
)

var (
	ErrReading        = errors.New("read error")
	ErrInvalidRequest = errors.New("invalid request")
	ErrInvalidNum     = errors.New("invalid number")
)

type Parser struct {
	handler *handler.Handler
}

func NewParser(handler *handler.Handler) *Parser {
	return &Parser{
		handler: handler,
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

func (p *Parser) HandleConn(conn net.Conn) {
	client := &model.Client{
        Conn:      conn,
        WakeUpChan: make(chan *model.WakeUpData, 1),
    }

	defer func() {
		close(client.WakeUpChan)
		client.Conn.Close()
	}()

	reader := bufio.NewReader(client.Conn)
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
					// fmt.Println("arg=", arg)
					args = append(args, arg)
				}
			}

			p.handler.HandleArgs(client, args...)

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

			p.handler.HandleArgs(client, arg)

		default:
			reader.ReadBytes('\n')
			conn.Write([]byte("-ERR Protocol error: unexpected first byte\r\n"))
			return
		}
	}
}
