package main

import (
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
        conns: make(map[string]net.Conn),
    }
}

func (s *Server) hanldeMessages() {
    for msg := range s.msg{
        //TODO request = _
        _, err := api.ParseRequest(msg.Content)
        if err != nil{
            msg.Conn.Write([]byte("malformed request"))
            return
        }
    }
}
 
func (s *Server) handleConnection(conn net.Conn)  error{
    defer conn.Close()
    addr := conn.RemoteAddr().String()
    s.mu.Lock()
    s.conns[addr] = conn
    s.mu.Unlock()
    buff := make([]byte, 1024)
    for{
        _, err := conn.Read(buff)
        if errors.Is(err, io.EOF){

            s.mu.Lock()
            delete(s.conns, addr)
            s.mu.Unlock()

            log.Printf("%s disconnected", conn.RemoteAddr().String())
            break
        } else if err != nil{
            log.Println("err from ", addr, err)
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
    go s.hanldeMessages()
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

