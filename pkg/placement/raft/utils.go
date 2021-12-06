package raft

import (
	"bytes"
	"net"
	"os"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/pkg/errors"
)

const defaultDirPermission = 0755

var (
	ErrEmptyRaftAddress = errors.New("empty raft address")
)

func ensureDir(dirName string) error {
	info, err := os.Stat(dirName)
	if !os.IsNotExist(err) && !info.Mode().IsDir() {
		return errors.New("file already existed")
	}

	err = os.Mkdir(dirName, defaultDirPermission)
	if err == nil || os.IsExist(err) {
		return nil
	}
	return err
}

func makeRaftLogCommand(t CommandType, member State) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(buf, &codec.MsgpackHandle{}).Encode(member)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func marshalMsgPack(in interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := codec.NewEncoder(buf, &codec.MsgpackHandle{})
	err := enc.Encode(in)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalMsgPack(in []byte, out interface{}) error {
	dec := codec.NewDecoderBytes(in, &codec.MsgpackHandle{})
	return dec.Decode(out)
}

func tryResolveRaftAdvertiseAddr(bindAddr string) (*net.TCPAddr, error) {
	// HACKHACK: Kubernetes POD DNS A record population takes some time
	// to look up the address after StatefulSet POD is deployed.
	var err error
	var addr *net.TCPAddr
	for retry := 0; retry < nameResolveMaxRetry; retry++ {
		addr, err = net.ResolveTCPAddr("tcp", bindAddr)
		if err == nil {
			return addr, nil
		}
		time.Sleep(nameResolveRetryInterval)
	}
	return nil, err
}
