package main

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/myselfBZ/my-kafka/api"
)


type Message struct{
    Content []byte
    Conn net.Conn
}

type Server struct{
    addr string
    quit chan struct{}
    msg chan Message

    conns map[string]net.Conn
    
    mu sync.Mutex
}

func NewServer(addr string) *Server{
    return &Server{
        addr: addr,
        msg: make(chan Message),
        quit: make(chan struct{}),
        mu: sync.Mutex{},
    }
}

func (s *Server) hanldeMessages() {
    for msg := range s.msg{
        // extract the api key
        apiKey := api.ApiKey(binary.BigEndian.Uint16(msg.Content[4:6]))
        switch apiKey{
        case api.PRODUCE:
            log.Println("produce")
        case api.FETCH:
            log.Println("fetch")
        }
    }
}
 
func (s *Server) handleConnection(conn net.Conn)  error{
    s.mu.Lock()
    s.conns[conn.RemoteAddr().String()] = conn
    s.mu.Unlock()
    buff := make([]byte, 1024)
    for{
        _, err := conn.Read(buff)
        if errors.Is(err, io.EOF){
            log.Printf("%s disconnected", conn.RemoteAddr().String())
            break
        }
        s.msg <- Message{ Content: buff, Conn: conn }

    }
    return nil
}

func (s *Server) run(){
    ln, err := net.Listen("tcp", "localhost:6969")
    if err != nil{
        log.Fatal(err)
    }
    go s.accept(ln)
    <-s.quit
    os.Exit(0)
}

func (s *Server) accept(ln net.Listener) {
    for {
        conn, err := ln.Accept()
        if err != nil{
            log.Println("couldnt connect", err)
        }
        go s.handleConnection(conn)
    }
}

func main(){
    s := NewServer("localhost:6969")
    s.run()
}



