/*
| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
--- 发送的报文：----
| Option | Header1 | Body1 | Header2 | Body2 | ...
*/

package geerpc

import (
	"encoding/json"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"

	"github.com/sirupsen/logrus"
)

// MagicNumber 标记这是一个 geeRPC 请求
const MagicNumber = 0x3bef5c

// Option 包含了 geeRPC 的协商信息
// 为了简化，协商信息固定使用 JSON 编码
type Option struct {
	MagicNumber int        // MagicNumber 标记了 geeRPC 请求
	CodecType   codec.Type // 客户端选择的编解码方式
}

// DefaultOption 是一个默认的协商配置
var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server 代表一个 RPC 服务器
type Server struct{}

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
	s.serveCodec(f(conn))
}



var invalidRequest = struct{}{}
// serveCodec 是核心的请求处理循环
//读取请求 处理请求 回复请求
func (s *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	// 用于处理并发请求的等待组 wait group
	wg := new(sync.WaitGroup) 
	for {
		req,err := s.readRequest(cc)
		if err != nil{ //读取失败 发送空的请求体
			if req == nil{ //请求头读取失败
				break 
			}
			req.h.Error = err.Error()
			s.sendResponse(cc,req.h, invalidRequest,sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc,req,sending,wg)
	}
	wg.Wait()	
	_ = cc.Close()
}

//记录rpc调用的header以及参数
type request struct{
	h *codec.Header
	argv,replyv reflect.Value
}

func (server *Server) readRequestHeader(cc codec.Codec)(*codec.Header,error){
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil{
		if err!=io.EOF && err != io.ErrUnexpectedEOF{
			logrus.Error("rpc server: read header failed: ", err)
			return nil,err
		}
	}
	return &h,nil
}

//返回rpc调用的所有信息
func (server Server) readRequest(cc codec.Codec)(*request,error){
	h,err := server.readRequestHeader(cc)
	if err != nil{
		logrus.Error("rpc server: read request header failed: ", err)
		return nil,err
	}
	req := &request{
		h: h,
	}
	//TODO: 类型还没确定
	req.argv = reflect.New(reflect.TypeOf(req.h.ServiceMethod))
	req.replyv = reflect.New(reflect.TypeOf(req.h.ServiceMethod))
	if err := cc.ReadBody(req.argv.Interface()); err != nil {
		logrus.Error("rpc server: read request body failed: ", err)
		return nil,err
	}
	return req,nil
}

func (server *Server) sendResponse(cc codec.Codec,h *codec.Header,body interface{},sending *sync.Mutex){
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h,body); err != nil{
		logrus.Error("rpc server: write response failed: ", err)
	}
}

//进行rpc调用
func (server *Server) handleRequest(cc codec.Codec,req *request,sending *sync.Mutex,wg *sync.WaitGroup){
	defer wg.Done()
	//TODO: 类型还没确定
	logrus.Infof("rpc server: handle request %s", req.h.ServiceMethod)
	req.replyv = reflect.ValueOf(fmt.Sprintf("geerpc resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
}