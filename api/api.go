package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)



type (
    ApiKey uint16
    ApiVer uint16
    CorrID uint32
    ClientIDLen uint16
)


const(
    FETCH ApiKey = 1
    PRODUCE ApiKey = 0
)

type Request interface{
    Headers() *RequestHeader
    Deserialize(*bytes.Buffer) error
}

type RequestHeader struct{
    Length uint32
    ApiKey ApiKey
    ApiVersion ApiVer
    CorrID CorrID
    ClientID string
}


func ParseRequest(data []byte) (Request, error) {
    buff := bytes.NewBuffer(data)
    header, err :=  parseHeader(buff)   

    if err != nil{
        return nil, err
    }

    var req Request

    switch header.ApiKey {
    case PRODUCE:
        req = &ProduceRequest{header: header}
    case FETCH:
        req = &FetchRequest{headers: header}
    default:
        return nil, errors.New("unknown api key")
    }

    if err := req.Deserialize(buff); err != nil{
        return nil, err
    }



    return req, nil
}

func parseHeader(buffer *bytes.Buffer) (*RequestHeader, error) {
    var h RequestHeader

    if err := binary.Read(buffer, binary.BigEndian, &h.Length); err != nil{
        return nil, err
    }

    log.Println("request legnth: ", h.Length)

    if err := binary.Read(buffer, binary.BigEndian, &h.ApiKey); err != nil {
        return nil, err
    }

    if err := binary.Read(buffer, binary.BigEndian, &h.ApiVersion); err != nil {
        return nil, err
    }

    if err := binary.Read(buffer, binary.BigEndian, &h.CorrID); err != nil {
        return nil, err
    }

    var clientIdLen ClientIDLen

    if err := binary.Read(buffer, binary.BigEndian, &clientIdLen); err != nil {
        return nil, err
    }

    if clientIdLen > 0 {
        clientIDBytes := make([]byte, clientIdLen)
        if _, err := buffer.Read(clientIDBytes); err != nil {
            return nil, err
        }
        h.ClientID = string(clientIDBytes)
    }

    log.Println("client id: ", h.ClientID)

    return &h, nil
}

