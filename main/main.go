package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"geerpc"
	"geerpc/codec"

	"github.com/sirupsen/logrus"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		logrus.Fatal("rpc server: listen failed: ", err)
	}
	logrus.Info("rpc server: listen on ", l.Addr())
	addr <- l.Addr().String()
	geerpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	conn, _ := net.Dial("tcp", <-addr)
	defer func() {
		_ = conn.Close()
	}()
	time.Sleep(time.Second)

	_ = json.NewEncoder(conn).Encode(geerpc.DefaultOption)
	cc := codec.NewGobCodec(conn)

	// send request
	for i := 0; i < 6; i++ {
		h := &codec.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		_ = cc.Write(h, fmt.Sprintf("geerpc req #%d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		logrus.Info("reply: ", reply)
	}
}
