// simple_load.go
package main

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// проверить этот код
func main() {
    var ops uint64
    done := make(chan bool)
    
    // Запускаем нагрузку на 30 секунд
    go func() {
        for i := 0; i < 100; i++ {  // 100 соединений
            go func(id int) {
                conn, _ := net.Dial("tcp", "127.0.0.1:6379")
                defer conn.Close()
                writer := bufio.NewWriter(conn)
                key := fmt.Sprintf("key:%d", id)
                
                for {
                    select {
                    case <-done:
                        return
                    default:
                        // LPUSH
                        cmd := fmt.Sprintf("*3\r\n$5\r\nlpush\r\n$%d\r\n%s\r\n$3\r\nval\r\n", 
                            len(key), key)
                        writer.WriteString(cmd)
                        writer.Flush()
                        atomic.AddUint64(&ops, 1)
                        time.Sleep(time.Microsecond)  // небольшая пауза
                    }
                }
            }(i)
        }
        
        time.Sleep(30 * time.Second)
        close(done)
    }()
    
    // Показываем статистику каждую секунду
    ticker := time.NewTicker(1 * time.Second)
    for range ticker.C {
        current := atomic.LoadUint64(&ops)
        fmt.Printf("Operations: %d (%.0f/sec)\n", current, float64(current)/float64(time.Since(start).Seconds()))
    }
}

var start = time.Now()