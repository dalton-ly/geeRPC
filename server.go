/*
| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
--- 发送的报文：----
| Option | Header1 | Body1 | Header2 | Body2 | ...
*/

package geerpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"geerpc/codec"

	"github.com/sirupsen/logrus"
)

// MagicNumber 标记这是一个 geeRPC 请求
const MagicNumber = 0x3bef5c

// Option 包含了 geeRPC 的协商信息
// 为了简化，协商信息固定使用 JSON 编码
type Option struct {
	MagicNumber    int           // MagicNumber 标记了 geeRPC 请求
	CodecType      codec.Type    // 客户端选择的编解码方式
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

// DefaultOption 是一个默认的协商配置
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server 代表一个 RPC 服务器
type Server struct {
	servicesMap sync.Map
}

// NewServer 返回一个新的 Server 实例
func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是一个默认的服务器实例
var DefaultServer = NewServer()

// Accept 接收监听器上的连接，并为每个连接启动一个服务协程
func (s *Server) Accept(lis net.Listener) {
	// for 循环等待 socket 连接建立，并开启子协程处理
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		// 使用 go 关键字开启一个子协程
		go s.ServeConn(conn)
	}
}

// Accept 是对 DefaultServer.Accept 的一个便利封装
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// ServeConn 在单个连接上运行服务器
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	// 在函数退出时确保关闭连接
	defer func() { _ = conn.Close() }()

	var opt Option
	// 使用 json.Decoder 解码连接中的协商信息
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}

	// 检查 MagicNumber 是否匹配
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}

	// 根据客户端指定的 CodecType 获取对应的编解码器构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	// 使用构造函数创建编解码器实例，并调用 serveCodec 开始处理请求
	s.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

// serveCodec 是核心的请求处理循环
// 读取请求 处理请求 回复请求
func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)
	// 用于处理并发请求的等待组 wait group
	wg := new(sync.WaitGroup)
	for {
		req, err := s.readRequest(cc)
		if err != nil { // 读取失败 发送空的请求体
			if req == nil { // 请求头读取失败
				break
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// 记录rpc调用的header以及参数
type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			logrus.Error("rpc server: read header failed: ", err)
		}
		return nil, err
	}
	return &h, nil
}

// 返回rpc调用的所有信息
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		if err != io.EOF {
			logrus.Error("rpc server: read request header failed: ", err)
		}
		return nil, err
	}
	req := &request{
		h: h,
	}
	req.svc, req.mtype, err = server.findService(req.h.ServiceMethod)
	if err != nil {
		logrus.Error("rpc server: find service failed: ", err)
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// argvi需要是指针
	argvi := req.argv.Interface()
	if req.argv.Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err := cc.ReadBody(argvi); err != nil {
		logrus.Error("rpc server: read request body failed: ", err)
		return nil, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		logrus.Error("rpc server: write response failed: ", err)
	}
}

// 进行rpc调用
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{}, 1)
	sent := make(chan struct{}, 1)
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		logrus.Infof("rpc server: handle request %s", req.h.ServiceMethod)
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()
	if timeout == 0 { // 没有设置超时时间
		<-called
		<-sent
		return
	}
	select {
	case <-called:
		<-sent
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	}

	// req.replyv = reflect.ValueOf(fmt.Sprintf("geerpc resp %d", req.h.Seq))
}

func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.servicesMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc server: service already registered: " + s.name)
	}
	return nil
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// 通过Service.Method 格式来查找服务
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service.method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svic, ok := server.servicesMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svic.(*service) // 断言
	mtype = svc.methods[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// http协议支持
const (
	connected        = "200 Connected to Gee RPC"
	defaultHTTPPath  = "/_geerpc_"
	defaultDebugPath = "/debug/geerpc"
)

// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		logrus.Error("rpc server: hijack failed: ", err)
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.ServeConn(conn)
}

func (s *Server) HandleHTTP() {
	http.Handle(defaultHTTPPath, s)
	http.Handle(defaultDebugPath, debugHTTP{s})
	logrus.Infof("rpc server: http service exposed at %s", defaultHTTPPath)
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
