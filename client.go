package geerpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

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

// Dial connects to an RPC server at the specified network address
func Dial(network, address string, opts ...*Option) (*Client, error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
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

func (client *Client) GO(serviceMethod string, args, reply interface{}, done chan *Call) {
	if done == nil {
		done = make(chan *Call, 10)
	}
	call := &Call{
		Method: serviceMethod,
		Args:   args,
		Reply:  reply,
		Done:   done,
	}
	client.send(call)
}

func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	done := make(chan *Call, 10)
	client.GO(serviceMethod, args, reply, done)
	call := <-done
	return call.Error
}
