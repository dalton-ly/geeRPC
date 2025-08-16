package geerpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"geerpc/codec"

	"github.com/sirupsen/logrus"
)

// 单词RPC调用的元数据
type Call struct {
	Seq    uint64      // 请求序号
	Method string      // 方法名
	Args   interface{} // 请求参数
	Reply  interface{} // 回复参数
	Error  error       // 错误信息
	Done   chan *Call  // 用于通知调用完成 当调用结束时，会调用 call.done() 通知调用方。
}

func (call *Call) done() {
	call.Done <- call
}

// 客户端连接示例
type Client struct {
	cc       codec.Codec // 用于解码
	opt      *Option
	sending  sync.Mutex
	header   codec.Header // 每个请求的头信息
	mu       sync.Mutex
	seq      uint64
	pending  map[uint64]*Call // 未完成的请求
	closing  bool             // 用户主动关闭
	shutdown bool             // 发生异常shutdown
}

var _ io.Closer = (*Client)(nil) // 在编译时验证Client结构体是否实现了io.Closer接口（即是否存在Close()方法）

var ErrShutdown = errors.New("connection is shutdown")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// 检查client是否可用
func (client *Client) isAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 注册到pending中
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.seq++
	client.pending[call.Seq] = call

	return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err := client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = errors.New(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = err
			}
			call.done()
		}
	}
	client.terminateCalls(err)
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		logrus.Errorf("invalid codec type %s", opt.CodecType)
		return nil, err
	}
	// send optinos with server
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		logrus.Errorf("send options failed: %v", err)
		conn.Close()
		return nil, err
	}

	// //握手信号
	// buf := make([]byte, len("GEERPC_CONN_OK"))
	// if _, err := io.ReadFull(conn, buf); err != nil {
	// 	logrus.Println("handshake failed:", err)
	// 	return nil, err
	// }
	// if string(buf) != "GEERPC_CONN_OK" {
	// 	return nil, errors.New("handshake failed")
	// }
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

type newClientFunc func(conn net.Conn, opt *Option) (*Client, error)

type clientResult struct {
	client *Client
	err    error
}

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err = f(conn, opt)
		ch <- clientResult{client, err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, errors.New("connect timeout")
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial connects to an RPC server at the specified network address
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	client.header.Seq = seq
	client.header.ServiceMethod = call.Method
	client.header.Error = ""

	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) GO(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		logrus.Fatal("RPC client: done channel is unbuffered")
	}
	call := &Call{
		Method: serviceMethod,
		Args:   args,
		Reply:  reply,
		Done:   done,
	}
	client.send(call)
	return call
}

func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	done := make(chan *Call, 1)
	call := client.GO(serviceMethod, args, reply, done)
	select {
	case <-ctx.Done(): // 调用超时
		client.removeCall(call.Seq)
		return errors.New("RPC client: call failed: " + ctx.Err().Error())
	case <-call.Done:
		return call.Error //
	}
}

func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, "CONNECT "+defaultHTTPPath+" HTTP/1.0\n\n")

	//
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil {
		return nil, err
	}
	if resp.Status != "200 Connected to Gee RPC" {
		return nil, errors.New("unexpected HTTP response: " + resp.Status)
	}
	return NewClient(conn, opt)
}

func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// XDial calls different functions to connect to a RPC server
// according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts...)
	}
}
