package partition

import (
	"errors"
	"sync"
)

type Message struct{
    Offset uint
    content []byte 
}

func NewPartition(index uint) *Partition {
    return &Partition{
        Index: index,
        mu: sync.Mutex{},
        nextOffset: 0,
    }
}


type Partition struct{
    Index uint
    mu  sync.Mutex
    nextOffset uint
    messages []*Message
}


// return batch of messages and the offset 
func (p *Partition) Read(startOffset uint, maxBytes uint) ([]*Message, int, error) {
    var result []*Message

    p.mu.Lock()
    defer p.mu.Unlock()



    if len(p.messages) < int(startOffset) {
        return nil, -1, errors.New("invalid offset")
    }

    msgSize := 0

    for _, msg := range p.messages {
        msgSize += len(msg.content)
        if msgSize >= int(maxBytes) {
            break
        }
        result = append(result, msg)
    }

    return result, int(result[len(result) - 1].Offset) + 1, nil
}

func (p *Partition) Write(msg []byte) (uint) {
    p.mu.Lock()
    defer p.mu.Unlock()



    p.messages = append(p.messages, &Message{content: msg, Offset: p.nextOffset})
    p.nextOffset++
    return p.nextOffset
}
