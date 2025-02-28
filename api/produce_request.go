package api

import (
	"bytes"
	"encoding/binary"
	"log"
)

type MessageMetaData struct{
    Offset uint64
    MessageSize uint32
    CRC uint32
    Magic uint8
    Attributes uint8
}

type Message struct{
    MetaData MessageMetaData
    Key string
    Value string
}

type Partition struct{
    Index uint32
    MessageSetSize uint32
    Messages []*Message
}

type Topic struct{
    Name string
    Paritions []*Partition
}


type ProduceRequest struct {
	Header *RequestHeader
	Acks   uint16
    Timeout uint32
    TopicNum uint32
    Topics []Topic
}



func (p *ProduceRequest) readMessage(buff *bytes.Buffer) *Message {
    var msg Message
    binary.Read(buff, binary.BigEndian, &msg.MetaData.Offset)
    binary.Read(buff, binary.BigEndian, &msg.MetaData.MessageSize)
    binary.Read(buff, binary.BigEndian, &msg.MetaData.CRC)
    binary.Read(buff, binary.BigEndian, &msg.MetaData.Magic)
    binary.Read(buff, binary.BigEndian, &msg.MetaData.Attributes)
    
    log.Println("offset: ", msg.MetaData.Offset)
    var keyLen int32
    binary.Read(buff, binary.BigEndian, &keyLen)

    if keyLen > 0 {
        key := make([]byte, keyLen)
        buff.Read(key)
        msg.Key = string(key)
    }

    var valueLen uint32
    binary.Read(buff, binary.BigEndian, &valueLen)
    value := make([]byte, valueLen)
    buff.Read(value)
    msg.Value = string(value)
    log.Println("value: ", msg.Value)
    return &msg
}

func (p *ProduceRequest) Deserialize(buff *bytes.Buffer) {
    binary.Read(buff, binary.BigEndian, &p.Acks)
    binary.Read(buff, binary.BigEndian, &p.Timeout)
    binary.Read(buff, binary.BigEndian, &p.TopicNum)

    for i := 0; i < int(p.TopicNum); i++{
        var topic Topic
        var topicNameLen uint16
        binary.Read(buff, binary.BigEndian, &topicNameLen)
        topicName  := make([]byte, topicNameLen)
        buff.Read(topicName)
        topic.Name = string(topicName)

        var partitionNum uint32
        binary.Read(buff, binary.BigEndian, &partitionNum)

        for pt := 0; pt < int(partitionNum); pt++ {
            var part Partition
            binary.Read(buff, binary.BigEndian, &part.Index)

            binary.Read(buff, binary.BigEndian, &part.MessageSetSize)
            readMsg := 0
            for mg := 0; mg < int(part.MessageSetSize); mg+=readMsg{
                msg := p.readMessage(buff)
                readMsg += int(msg.MetaData.MessageSize)
                part.Messages = append(part.Messages, msg)
            }

            topic.Paritions = append(topic.Paritions, &part)
        }

        p.Topics = append(p.Topics, topic)

    }
}

func (p *ProduceRequest) Headers() *RequestHeader {
    return p.Header
}
