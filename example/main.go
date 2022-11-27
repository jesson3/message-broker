package main

import (
	"fmt"
	"net"

	"github.com/jesson3/message-broker/broker"
)

func main() {
	listen, err := net.Listen("tcp", "127.0.0.1:9876")
	if err != nil {
		fmt.Println("listen failed, err:", err)
		return
	}
	go broker.Save()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed, err:", err)
			continue
		}
		go broker.Process(conn)
	}
}