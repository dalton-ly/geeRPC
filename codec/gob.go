package codec

import (
	"bufio"
	"encoding/gob"
	"io"

	"github.com/sirupsen/logrus"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *GobCodec) Write(h *Header, body interface{}) error {
	defer func(){
		err := c.buf.Flush()
		if err != nil{
			_ = c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		logrus.Error("gob codec: encode header failed: ", err)
		return err
	}

	if err := c.enc.Encode(body); err != nil {
		logrus.Error("gob codec: encode body failed: ", err)
		return err
	}
	return nil
}




func NewGobCodec(conn io.ReadWriteCloser) Codec {
	return &GobCodec{
		conn: conn,
		buf:  bufio.NewWriter(conn),
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(conn),
	}
}
