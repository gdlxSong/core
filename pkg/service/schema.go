package service

import (
	"context"

	pb "github.com/tkeel-io/core/api/core/v1"
	apim "github.com/tkeel-io/core/pkg/manager"
	"go.uber.org/atomic"
)

type schemaService struct {
	pb.UnimplementedSearchServer

	inited     *atomic.Bool
	apiManager apim.APIManager
}

func (s *schemaService) Init(apiManager apim.APIManager) {
	s.apiManager = apiManager
	s.inited.Store(true)
}

func (s *schemaService) CreateSchema(ctx context.Context, in *pb.CreateSchemaRequest) (*pb.CreateSchemaResponse, error) {
	return nil, nil
}
