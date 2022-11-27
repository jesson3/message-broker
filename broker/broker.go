package broker

import (
	"bufio"
	"net"
	"sync"
)

var topics = sync.Map{}

func handleErr(conn net.Conn) {
	if err := recover(); err != nil {
		println(err.(string))
		conn.Write(MsgToBytes(Msg{MsgType: 4}))
	}
}

func Process(conn net.Conn) {
	defer handleErr(conn)
	reader := bufio.NewReader(conn)
	msg := BytesToMsg(reader)
	queue, ok := topics.Load(msg.Topic)
	var res Msg
	if msg.MsgType == 1 {
		// consumer
		if queue == nil || queue.(*Queue).len == 0 {
			return
		}
		msg = queue.(*Queue).poll()
		msg.MsgType = 1
		res = msg
	} else if msg.MsgType == 2 {
		// producer
		if !ok {
			queue = &Queue{}
			queue.(*Queue).data.Init()
			topics.Store(msg.Topic, queue)
		}
		queue.(*Queue).offer(msg)
		res = Msg{Id: msg.Id, MsgType: 2}
	} else if msg.MsgType == 3 {
		// consumer ack
		if queue == nil {
			return
		}
		queue.(*Queue).delete(msg.Id)
	}

	conn.Write(MsgToBytes(res))
}
