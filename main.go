package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
)


func main(){
    ln, err := net.Listen("tcp", "localhost:6969")
    if err != nil{
        log.Fatal(err)
    }
    for {
        conn, err := ln.Accept()

        if err != nil{
            log.Fatal(err)
        }

        buf := make([]byte, 1024)
        _, err = conn.Read(buf)

        if err != err{
            log.Fatal(err)
        }

        handleHeader(buf)
    }

}



type RequestHeader struct{
    Lenght uint32
    ApiKey uint16
    ApiVer uint16
    CorrId uint32
    ClientIDLen uint16
    ClientID  []byte
}



// TODO, make the errors more graceful!!
func handleHeader(d []byte) *RequestHeader {
    buffer := bytes.NewBuffer(d)

    var h RequestHeader

    if err := binary.Read(buffer, binary.BigEndian, &h.Lenght); err != nil{
        log.Fatal(err)     
    }

    if err := binary.Read(buffer, binary.BigEndian, &h.ApiKey); err != nil{
        log.Fatal(err)     
    }

    if err := binary.Read(buffer, binary.BigEndian, &h.ApiVer); err != nil{
        log.Fatal(err)     
    }


    if err := binary.Read(buffer, binary.BigEndian, &h.CorrId); err != nil{
        log.Fatal(err)     
    }


    if err := binary.Read(buffer, binary.BigEndian, &h.ClientIDLen); err != nil{
        log.Fatal(err)     
    }


    if err := binary.Read(buffer, binary.BigEndian, &h.ClientID); err != nil{
        log.Fatal(err.Error())
    }
    return &h
}



