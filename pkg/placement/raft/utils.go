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

func ensureDir(dirName string) error {
	info, err := os.Stat(dirName)
	if !os.IsNotExist(err) && !info.Mode().IsDir() {
		return errors.New("file already existed")
	}

	err = os.Mkdir(dirName, defaultDirPermission)
	if err == nil || os.IsExist(err) {
		return nil
	}
	return errors.Wrap(err, "create directory failed")
}

func makeRaftLogCommand(t CommandType, data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(buf, &codec.MsgpackHandle{}).Encode(data)
	if err != nil {
		return nil, errors.Wrap(err, "make Raft LogCommand failed")
	}
	return buf.Bytes(), nil
}

func marshalMsgPack(in interface{}) ([]byte, error) { //nolint
	buf := bytes.NewBuffer(nil)
	enc := codec.NewEncoder(buf, &codec.MsgpackHandle{})
	if err := enc.Encode(in); err != nil {
		return nil, errors.Wrap(err, "marshal data failed")
	}
	return buf.Bytes(), nil
}

func unmarshalMsgPack(in []byte, out interface{}) error {
	dec := codec.NewDecoderBytes(in, &codec.MsgpackHandle{})
	return errors.Wrap(dec.Decode(out), "unmarshal data failed")
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
	return nil, errors.Wrap(err, "resolve dns failed")
}
