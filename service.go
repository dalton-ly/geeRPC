package geerpc

import (
	"go/ast"
	"reflect"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

//描述一个可以被远程调用的方法的所有信息。
type methodType struct {
    method    reflect.Method // 方法本身
    ArgType   reflect.Type   // 第一个参数的类型 (请求参数)
    ReplyType reflect.Type   // 第二个参数的类型 (返回参数)
    numCalls  uint64         // 方法被调用的次数 (用于统计)
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// newArgv 创建一个新的参数值 然后通过反序列化来修改这个值?
func (m *methodType) newArgv() reflect.Value{
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else{
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyv() reflect.Value{
	//replyv 是一个指针
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Ptr:
		replyv = reflect.New(m.ReplyType.Elem())
	case reflect.Slice:
		replyv = reflect.MakeSlice(m.ReplyType, 0, 0)
	}
	return replyv
}

//service

type service struct {
    name    string                 // 映射的结构体的名称, e.g., "Arith"
    typ     reflect.Type           // 结构体的类型信息 (类似一个类型蓝图)
    rcvr    reflect.Value          // 结构体的实例本身 (持有具体的值)
    methods map[string]*methodType // 该服务所有符合RPC规范的方法
}

func newService(rcvr interface{}) *service{
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	if !ast.IsExported(s.name){
		logrus.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}


// RPC 方法的规范必须是 func (t *T) MethodName(arg T1, reply *T2) error 形式。
func (s *service) registerMethods(){
	s.methods = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.methods[method.Name] = &methodType{
			method: method,
			ArgType:   argType,
			ReplyType: replyType,
		}
	}
	logrus.Infof("rpc server: %s register %d methods", s.name, len(s.methods))
}


func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}


func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv}) //在 Go 中，调用一个方法 myStruct.MyMethod() 时，myStruct 本身就是第一个隐式参数。反射调用时，必须把它作为第一个参数显式地传递进去。
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

