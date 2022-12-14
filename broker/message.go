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
	Topic    string
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
	// topic
	msgLastIndex := 16 + m.TopicLen
	m.Topic = string(buf[16:msgLastIndex])
	// msgtype
	buff = bytes.NewBuffer(buf[msgLastIndex : msgLastIndex+8])
	binary.Read(buff, binary.LittleEndian, &m.MsgType)

	buff = bytes.NewBuffer(buf[msgLastIndex+8 : msgLastIndex+16])
	binary.Read(buff, binary.LittleEndian, &m.Len)

	if m.Len <= 0 {
		return m
	}

	m.Payload = buf[msgLastIndex+16:]
	return m
}

func MsgToBytes(msg Msg) []byte{
	msg.TopicLen = int64(len([]byte(msg.Topic)))
	msg.Len = int64(len([]byte(msg.Payload)))

	var data []byte
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.Id)
	data = append(data, buf.Bytes()...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.TopicLen)
	data = append(data, buf.Bytes()...)

	data = append(data, []byte(msg.Topic)...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.MsgType)
	data = append(data, buf.Bytes()...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.Len)
	data = append(data, buf.Bytes()...)
	data = append(data, []byte(msg.Payload)...)

	return data
}