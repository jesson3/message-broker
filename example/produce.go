package main

import (
	"fmt"
	"net"

	"github.com/jesson3/message-broker/broker"
)

func produce() {
	conn, err := net.Dial("tcp", "127.0.0.1:9876")
	if err != nil {
		fmt.Println("connect failed, err:", err)
	}
	defer conn.Close()

	msg := broker.Msg{Id: 1102, Topic: "topic test", MsgType: 2, Payload: []byte("it is mine")}
	n, err:= conn.Write(broker.MsgToBytes(msg))
	if err != nil {
		fmt.Println("write failed, err:", err)
	}

	fmt.Print(n)
}
