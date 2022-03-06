package v1

import (
	"fmt"

	"github.com/golang/protobuf/proto"
)

// definition message header field.
const (
	META_OP       = "x-msg-op"
	META_TTL      = "x-msg-ttl"
	META_TYPE     = "x-msg-type"
	META_ENTITYID = "x-msg-enid"
	META_VERSION  = "x-msg-version"
)

type EventType string

const (
	ETCache    EventType = "core.event.Cache"
	ETEntity   EventType = "core.event.Entity"
	ETSystem   EventType = "core.event.System"
	ETCallback EventType = "core.event.Callback"
)

type SystemOp string

const (
	OpCreate SystemOp = "core.event.System.Create"
	OpDelete SystemOp = "core.event.System.Delete"
)

type Attribution interface {
	Attr(key string) string
	SetAttr(key string, value string) Event
	ForeachAttr(handler func(key, val string))
}

type Event interface {
	Attribution

	ID() string
	Copy() Event
	Type() EventType
	Version() string
	Validate() error
	Entity() string
	SetEntity(entityId string) Event
	SetTTL(td int) Event

	RawData() []byte
	Payload() isProtoEvent_Data
	SetPayload(payload isProtoEvent_Data) Event
	CallbackAddr() string
}

func (e *ProtoEvent) ID() string {
	return e.Id
}

func (e *ProtoEvent) Copy() Event {
	return e
}

func (e *ProtoEvent) Type() EventType {
	return EventType(e.Metadata[META_TYPE])
}

func (e *ProtoEvent) Version() string {
	return e.Metadata[META_VERSION]
}

func (e *ProtoEvent) Validate() error {
	return nil
}

func (e *ProtoEvent) Entity() string {
	return e.Metadata[META_ENTITYID]
}

func (e *ProtoEvent) SetEntity(entityId string) Event {
	e.Metadata[META_ENTITYID] = entityId
	return e
}

func (e *ProtoEvent) SetTTL(ttl int) Event {
	e.Metadata[META_TTL] = fmt.Sprintf("%d", ttl)
	return e
}

func (e *ProtoEvent) RawData() []byte {
	return e.GetRawData()
}

func (e *ProtoEvent) Payload() isProtoEvent_Data {
	return e.GetData()
}

func (e *ProtoEvent) SetPayload(payload isProtoEvent_Data) Event {
	e.Data = payload
	return e

}

func (e *ProtoEvent) Attr(key string) string {
	return e.Metadata[key]
}

func (e *ProtoEvent) SetAttr(key string, value string) Event {
	e.Metadata[key] = value
	return e
}

func (e *ProtoEvent) ForeachAttr(handler func(key, val string)) {
	for key, val := range e.Metadata {
		handler(key, val)
	}
}

func (e *ProtoEvent) CallbackAddr() string {
	return e.Callback
}

// ----------------------

type PatchEvent interface {
	Event
	Patches() []*PatchData
}

func (e *ProtoEvent) Patches() []*PatchData {
	switch data := e.Data.(type) {
	case *ProtoEvent_RawData:
		return []*PatchData{}
	case *ProtoEvent_Patches:
		return data.Patches.Patches
	}
	panic("invalid data type")
}

func Marshal(e Event) ([]byte, error) {
	ev, _ := e.(*ProtoEvent)
	return proto.Marshal(ev)
}

func Unmarshal(data []byte, e *ProtoEvent) error {
	return proto.Unmarshal(data, e)
}

//-----------------------------------------------

type SystemEvent interface {
	Event
	Action() *SystemData
}

func (e *ProtoEvent) Action() *SystemData {
	return e.GetSystemData()
}
