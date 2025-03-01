package topics

import (
	"errors"

	"github.com/myselfBZ/my-kafka/internal/partition"
)

type Topic struct{
    Name string
    Parts map[uint]*partition.Partition
}

func NewTopic(name string, partIndex int) *Topic {
    return &Topic{
        Name: name,
        Parts: map[uint]*partition.Partition{
            uint(partIndex):partition.NewPartition(uint(partIndex)),
        },
    }
}

func (t *Topic) Produce(partIndex uint, msg []byte) (offset uint, err error) {
    part, ok := t.Parts[partIndex]
    if !ok{
        return 0, errors.New("partition not found")
    }

    of := part.Write(msg)

    return of, nil
}


func (t *Topic) Consume(partIndex uint, startOffset uint, maxBytes uint) ([]*partition.Message, int, error){
    part, ok := t.Parts[partIndex]
    if !ok{
        return nil, 0, errors.New("partition not found")
    }

    messages, offSet, err := part.Read(startOffset, maxBytes)
    if err != nil{
        return nil, 0, err
    }

    return messages, offSet, nil
}
