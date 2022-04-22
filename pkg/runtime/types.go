package runtime

import (
	"context"

	"github.com/tkeel-io/core/pkg/mapper"
	"github.com/tkeel-io/core/pkg/repository/dao"
	xjson "github.com/tkeel-io/core/pkg/util/json"
	"github.com/tkeel-io/tdtl"
)

type Patch struct {
	Op    xjson.PatchOp
	Path  string
	Value *tdtl.Collect
}

type EntityAttr interface {
	Type() string
	Owner() string
	Source() string
	Version() int64
	LastTime() int64
	TemplateID() string
	Properties() tdtl.Node
	Scheme() tdtl.Node
	GetProp(key string) tdtl.Node
}

type Entity interface {
	EntityAttr

	ID() string
	Get(string) tdtl.Node
	Copy() Entity
	Tiled() tdtl.Node
	Handle(context.Context, *Feed) *Feed
	Basic() *tdtl.Collect
	Raw() []byte
}

type handlerImpl struct {
	fn func(context.Context, *Feed) *Feed
}

func (h *handlerImpl) Handle(ctx context.Context, feed *Feed) *Feed {
	if nil != feed.Err {
		return feed
	}
	return h.fn(ctx, feed)
}

type MCache struct {
	ID        string
	EntityID  string
	Mapper    mapper.Mapper
	Tentacles []mapper.Tentacler
}

type Task func()

type ExpressionInfo struct {
	// embeded Expression.
	dao.Expression

	isHere        bool // expression 所属 entity 是否属于当前 runtime.
	version       int
	subEndpoints  []SubEndpoint
	evalEndpoints []EvalEndpoint
}

type SubEndpoint struct {
	path         string
	target       string
	runtimeID    string
	expressionID string
}

func newSubEnd(path, target, exprID, runtimeID string) SubEndpoint {
	return SubEndpoint{
		path:         path,
		target:       target,
		runtimeID:    runtimeID,
		expressionID: exprID,
	}
}

func (s *SubEndpoint) ID() string {
	return s.runtimeID + s.path
}

func (s *SubEndpoint) String() string {
	return s.runtimeID + s.target + s.path
}

func (s *SubEndpoint) Expression() string {
	return s.expressionID
}

type EvalEndpoint struct {
	path        string
	target      string
	expresionID string
}

func newEvalEnd(path, target, expressionID string) EvalEndpoint {
	return EvalEndpoint{
		path:        path,
		target:      target,
		expresionID: expressionID,
	}
}

func (e EvalEndpoint) ID() string {
	return e.target + e.path
}

func (e *EvalEndpoint) String() string {
	return e.path + e.target
}

func (e *EvalEndpoint) Expression() string {
	return e.expresionID
}
