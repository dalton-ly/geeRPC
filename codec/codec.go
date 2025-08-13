// 编解码
package codec

import "io"

// 参数和消息放在body中，其余信息放在header中
type Header struct {
	ServiceMethod string // 服务名和方法名
	Seq           uint64 // 序号
	Error         string // 错误信息
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	//  gob 编码
	GobType Type = "application/gob"
	// json 编码
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
