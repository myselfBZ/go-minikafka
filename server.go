package main

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/myselfBZ/my-kafka/api"
	"github.com/myselfBZ/my-kafka/internal/topics"
)




type Message struct{
    Content []byte
    Conn net.Conn
}

type Server struct{
    addr string
    quit chan struct{}
    msg chan Message
    topicMu sync.Mutex
    topics map[string]*topics.Topic

    conns map[string]net.Conn
    
    mu sync.Mutex
}

func NewServer(addr string) *Server{
    return &Server{
        addr: addr,
        msg: make(chan Message),
        quit: make(chan struct{}),
        mu: sync.Mutex{},
        topicMu: sync.Mutex{},
        topics: map[string]*topics.Topic{
            "kafka":topics.NewTopic("kafka", 0),
        },
        conns: make(map[string]net.Conn),
    }
}


func writeMessages(clientTopic *api.Topic, serverTopic *topics.Topic) (uint, error) {
    var offset uint
    for _, p := range clientTopic.Paritions{
        for _, msg := range p.Messages{
            var err  error
            offset, err = serverTopic.Produce(uint(p.Index), []byte(msg.Value))
            if err != nil{
                return 0, err
            }
        }
    } 

    return offset, nil
}

func (s *Server) hanldeMessages() {
    for msg := range s.msg{
        //TODO request = _
        r, err := api.ParseRequest(msg.Content)

        if err != nil{
            msg.Conn.Write([]byte("malformed request"))
            return
        }
        switch req := r.(type) {

        case *api.ProduceRequest:
            for _, t := range req.Topics{
                topic, ok := s.topics[t.Name]
                if !ok{
                    s.topicMu.Lock()
                    topic = topics.NewTopic(t.Name, s.generatePartitionIndex())
                    s.topics[t.Name] = topic
                    s.topicMu.Unlock()
                }
                offset, err := writeMessages(&t, topic)
                if err != nil{
                    msg.Conn.Write([]byte(err.Error()))
                    return
                }

                log.Println("offset: ", offset)
            }
        case *api.FetchRequest:
            for _, topic := range req.Topics{
                serverTopic := s.topics[topic.Name]
                msgs, _, err := serverTopic.Consume(topic.Partitions[0].Index, topic.Partitions[0].FetchOffset, uint(topic.Partitions[0].MaxBytes))
                if err != nil{
                    msg.Conn.Write([]byte(err.Error()))
                    return
                }
                for _, m := range msgs {
                    if _, err := msg.Conn.Write(m.Content); err != nil{
                        log.Println("error writing to connection: ", err)
                    }
                }
            }
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

func (s *Server) Run() error {
    ln, err := net.Listen("tcp", "localhost:6969")
    if err != nil{
        log.Fatal(err)
    }
    go s.hanldeMessages()
    return s.accept(ln)    
}

func (s *Server) accept(ln net.Listener) error {
    for {
        conn, err := ln.Accept()
        if err != nil{
            log.Println("couldnt connect", err)
            continue
        }
        go s.handleConnection(conn)
    }
}

func (s *Server) generatePartitionIndex() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    totalPartitions := 0
    for _, t := range s.topics{
        partitions := len(t.Parts)
        totalPartitions += partitions
    }
    return totalPartitions + 1
}

func main(){
    s := NewServer("localhost:6969")
    s.Run()
}

