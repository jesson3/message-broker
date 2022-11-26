package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Msg struct {
	Id       int64
	TopicLen int64
	MsgType  int64
	Len      int64
	Payload  []byte
}

func BytesToMsg(reader io.Reader) Msg {
	m := Msg{}

	var buf [128]byte
	n, err := reader.Read(buf[:])
	if err != nil {
		fmt.Println()
	}
	fmt.Println("read bytes:", n)
	// id
	buff := bytes.NewBuffer(buf[0:8])
	binary.Read(buff, binary.LittleEndian, &m.Id)
	// topicLen
	buff = bytes.NewBuffer(buf[8:16])
	binary.Read(buff, binary.LittleEndian, &m.TopicLen)
	return m
}
