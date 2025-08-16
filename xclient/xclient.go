package xclient

import (
	"context"
	"io"
	"reflect"
	"sync"

	. "geerpc"
)


type XClient struct{
	d Discovery
	mode selectMode
	opt *Option
	mu sync.Mutex
	clients map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d Discovery, mode selectMode, opt *Option) *XClient {
	return &XClient{
		d: d,
		mode: mode,
		opt: opt,
		clients: make(map[string]*Client),
	}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, c := range xc.clients {
		c.Close()
		delete(xc.clients,key)
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	c, ok := xc.clients[rpcAddr]
	if ok && !c.IsAvailable() {
		_ = c.Close()
		delete(xc.clients, rpcAddr)
		c = nil
	}
	if c == nil{
		var err error
		c,err = XDial(rpcAddr, xc.opt)
		if err != nil{
			return nil, err
		}
		xc.clients[rpcAddr] = c
	}
	return c, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string,
args, reply interface{}) error{
	client ,err := xc.dial(rpcAddr)
	if err != nil{
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

//Broadcast 将请求广播到所有的服务实例，如果任意一个实例发生错误，则返回其中一个错误；如果调用成功，则返回其中一个的结果。有以下几点需要注意：
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply ==nil
	ctx ,cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers{
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var cloneReply interface{}
			if reply != nil{
				cloneReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, cloneReply)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && e ==nil{
				e = err
				cancel()
			}
			if err == nil && !replyDone{
				replyDone = true
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(cloneReply).Elem())
			}
		}(rpcAddr)
	}
	wg.Wait()
	return e
}