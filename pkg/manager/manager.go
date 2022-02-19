/*
Copyright 2021 The tKeel Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/pkg/errors"
	"github.com/tkeel-io/collectjs"
	"github.com/tkeel-io/core/pkg/config"
	"github.com/tkeel-io/core/pkg/constraint"
	"github.com/tkeel-io/core/pkg/dispatch"
	xerrors "github.com/tkeel-io/core/pkg/errors"
	zfield "github.com/tkeel-io/core/pkg/logger"
	"github.com/tkeel-io/core/pkg/manager/holder"
	"github.com/tkeel-io/core/pkg/mapper/tql"
	"github.com/tkeel-io/core/pkg/repository"
	"github.com/tkeel-io/core/pkg/repository/dao"
	"github.com/tkeel-io/core/pkg/runtime"
	"github.com/tkeel-io/core/pkg/runtime/message"
	"github.com/tkeel-io/core/pkg/runtime/state"
	"github.com/tkeel-io/core/pkg/runtime/subscription"
	"github.com/tkeel-io/core/pkg/types"
	"github.com/tkeel-io/core/pkg/util"
	"github.com/tkeel-io/kit/log"
	"go.uber.org/zap"
)

const eventSender = "Core.APIManager"
const respondFmt = "http://%s:%d/v1/respond"

type apiManager struct {
	holder     holder.Holder
	dispatcher dispatch.Dispatcher
	entityRepo repository.IRepository

	lock   sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func New(
	ctx context.Context,
	repo repository.IRepository,
	dispatcher dispatch.Dispatcher) (APIManager, error) {
	ctx, cancel := context.WithCancel(ctx)
	apiManager := &apiManager{
		ctx:        ctx,
		cancel:     cancel,
		holder:     holder.New(),
		entityRepo: repo,
		dispatcher: dispatcher,
		lock:       sync.RWMutex{},
	}

	return apiManager, nil
}

func (m *apiManager) Start() error {
	log.Info("start API Manager")
	return nil
}

func (m *apiManager) OnRespond(ctx context.Context, resp *holder.Response) {
	m.holder.OnRespond(resp)
}

// ------------------------------------APIs-----------------------------.

func (m *apiManager) checkID(base *Base) {
	if base.ID == "" {
		base.ID = util.UUID()
	}
}

func (m *apiManager) callbackAddr() string {
	return fmt.Sprintf(respondFmt, util.ResolveAddr(), config.Get().Proxy.HTTPPort)
}

// CreateEntity create a entity.
func (m *apiManager) CreateEntity(ctx context.Context, en *Base) (*Base, error) {
	var (
		err        error
		has        bool
		ev         cloudevents.Event
		templateID string
	)

	m.checkID(en)
	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.CreateEntity", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// 2. check template entity.
	if templateID, _ = ctx.Value(TemplateEntityID{}).(string); templateID != "" {
		if has, err = m.entityRepo.HasEntity(ctx, &dao.Entity{ID: templateID}); nil != err {
			log.Error("check template entity", zap.Error(err), zfield.Eid(templateID), zfield.ReqID(reqID))
			return nil, errors.Wrap(err, "create entity")
		} else if !has {
			log.Error("check template entity", zfield.Eid(en.ID), zfield.ReqID(reqID),
				zap.Error(xerrors.ErrTemplateNotFound), zfield.Template(templateID))
			return nil, errors.Wrap(xerrors.ErrTemplateNotFound, "create entity")
		}
	}

	// create event & set payload.
	if ev, err = m.makeEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		Properties: en.Properties,
	}); nil != err {
		log.Info("create entity", zfield.Eid(en.ID), zfield.Type(en.Type),
			zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "create entity")
	}

	// add event header fields.
	ev.SetExtension(message.ExtAPIRequestID, reqID)
	ev.SetExtension(message.ExtTemplateID, en.TemplateID)
	ev.SetExtension(message.ExtAPIIdentify, state.APICreateEntity.String())

	// dispatch event.
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("create entity, dispatch event", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "create entity, dispatch event")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("create entity", zfield.Eid(en.ID), zfield.ReqID(reqID),
			zap.Error(xerrors.New(resp.ErrCode)), zfield.Base(en.JSON()))
		return nil, xerrors.New(resp.ErrCode)
	}

	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("create entity, decode response", zfield.ReqID(reqID),
			zap.Error(err), zfield.Eid(en.ID), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "create entity, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

func (m *apiManager) UpdateEntity(ctx context.Context, en *Base) (*Base, error) {
	var (
		err error
		ev  cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.UpdateEntity", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source))

	// create event & set payload.
	if ev, err = m.makeEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		Properties: en.Properties,
		ConfigFile: en.ConfigFile,
	}); nil != err {
		log.Info("update entity", zfield.Eid(en.ID), zfield.Type(en.Type),
			zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source))
		return nil, errors.Wrap(err, "update entity")
	}

	ev.SetExtension(message.ExtAPIIdentify, state.APIUpdateEntity.String())
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("update entity", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "update entity")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("update entity", zfield.Eid(en.ID),
			zfield.ReqID(reqID), zap.Error(xerrors.New(resp.ErrCode)))
		return nil, xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("update entity, decode response",
			zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "update entity, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

// GetProperties returns Base.
func (m *apiManager) GetEntity(ctx context.Context, en *Base) (*Base, error) {
	var (
		err error
		ev  cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.GetProperties", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source))

	// create event & set payload.
	if ev, err = m.makeEvent(&dao.Entity{
		ID:     en.ID,
		Type:   en.Type,
		Owner:  en.Owner,
		Source: en.Source,
	}); nil != err {
		log.Info("get entity", zfield.Eid(en.ID), zfield.Type(en.Type),
			zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source))
		return nil, errors.Wrap(err, "get entity")
	}

	ev.SetExtension(message.ExtAPIIdentify, state.APIGetEntity.String())
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("get entity", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "get entity")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("get entity", zfield.Eid(en.ID),
			zfield.ReqID(reqID), zap.Error(xerrors.New(resp.ErrCode)))
		return nil, xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("get entity, decode response",
			zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "get entity, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

// DeleteEntity delete an entity from manager.
func (m *apiManager) DeleteEntity(ctx context.Context, en *Base) error {
	var (
		err error
		ev  cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.DeleteEntity", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// create event & set payload.
	if ev, err = m.makeEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		Properties: en.Properties,
	}); nil != err {
		log.Error("delete entity", zfield.Eid(en.ID), zfield.Type(en.Type),
			zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source))
		return errors.Wrap(err, "delete entity")
	}

	ev.SetExtension(message.ExtAPIIdentify, state.APIDeleteEntity.String())
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("delete entity, dispatch event", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return errors.Wrap(err, "delete entity, dispatch event")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("delete entity", zfield.Eid(en.ID),
			zfield.ReqID(reqID), zap.Error(xerrors.New(resp.ErrCode)))
		return xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("delete entity, decode response",
			zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return errors.Wrap(err, "delete entity, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return nil
}

// SetProperties set properties into entity.
func (m *apiManager) UpdateEntityProps(ctx context.Context, en *Base) (*Base, error) {
	var (
		err error
		ev  cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.UpdateEntityProps", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// create event & set payload.
	if ev, err = m.makeEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		Properties: en.Properties,
	}); nil != err {
		log.Info("set entity properties", zfield.Eid(en.ID), zfield.Type(en.Type),
			zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "set entity properties")
	}

	// set event header fields.
	ev.SetExtension(message.ExtAPIRequestID, reqID)
	ev.SetExtension(message.ExtAPIIdentify, state.APIUpdataEntityProps.String())

	// dispatch event.
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("set entity properties", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "set entity properties")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("set entity properties", zap.Error(xerrors.New(resp.ErrCode)),
			zfield.Eid(en.ID), zfield.ReqID(reqID), zfield.Base(en.JSON()))
		return nil, xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("set entity properties, decode response", zap.Error(err),
			zfield.ReqID(reqID), zfield.Eid(en.ID), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "set entity properties, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

func (m *apiManager) PatchEntityProps(ctx context.Context, en *Base, pds []state.PatchData) (*Base, error) {
	var (
		err error
		ev  cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.PatchEntity", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	if ev, err = m.makePatchEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		Properties: en.Properties,
	}, pds); nil != err {
		log.Error("make patch event", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "make patch event")
	}

	// set event header fields.
	ev.SetExtension(message.ExtAPIRequestID, reqID)
	ev.SetExtension(message.ExtAPIIdentify, state.APIPatchEntityProps.String())

	// dispatch event.
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("patch entity properties", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "patch entity properties")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("patch entity properties", zfield.Eid(en.ID),
			zap.Error(xerrors.New(resp.ErrCode)), zfield.Base(en.JSON()))
		return nil, xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("set entity properties, decode response",
			zap.Error(err), zfield.Eid(en.ID), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "set entity properties, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

func (m *apiManager) GetEntityProps(context.Context, *Base, []string) (*Base, error) {
	panic("implement me")
}

// SetProperties set properties into entity.
func (m *apiManager) UpdateEntityConfigs(ctx context.Context, en *Base) (*Base, error) {
	var (
		err   error
		bytes []byte
		ev    cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.UpdateEntityConfigs", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	if bytes, err = json.Marshal(en.Configs); nil != err {
		log.Error("json marshal configs", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "encode entity configs")
	}

	if ev, err = m.makeEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		ConfigFile: bytes,
	}); nil != err {
		log.Error("set entity configs", zfield.Eid(en.ID), zfield.Type(en.Type),
			zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "set entity configs")
	}

	ev.SetExtension(message.ExtAPIIdentify, state.APIUpdataEntityConfigs.String())
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("set entity configs, dispatch event", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "set entity configs, dispatch event")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("set entity configs", zfield.Eid(en.ID), zfield.ReqID(reqID),
			zap.Error(xerrors.New(resp.ErrCode)), zfield.Base(en.JSON()))
		return nil, xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("set entity configs, decode response",
			zfield.ReqID(reqID), zap.Error(err), zfield.Eid(en.ID), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "set entity configs, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

// PatchConfigs patch properties into entity.
func (m *apiManager) PatchEntityConfigs(ctx context.Context, en *Base, pds []state.PatchData) (*Base, error) {
	var (
		err error
		ev  cloudevents.Event
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.PatchConfigs", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// create event & set payload.
	if ev, err = m.makePatchEvent(&dao.Entity{
		ID:         en.ID,
		Type:       en.Type,
		Owner:      en.Owner,
		Source:     en.Source,
		Properties: en.Properties,
	}, pds); nil != err {
		log.Error("make patch event", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "make patch event")
	}

	// set event header fields.
	ev.SetExtension(message.ExtAPIIdentify, state.APIPatchEntityConfigs.String())
	if err = m.dispatcher.Dispatch(ctx, ev); nil != err {
		log.Error("patch entity configs", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "patch entity configs")
	}

	log.Debug("holding request, wait response", zfield.Eid(en.ID), zfield.ReqID(reqID))

	// hold request, wait response.
	resp := m.holder.Wait(ctx, reqID)
	if resp.Status != types.StatusOK {
		log.Error("set entity configs", zap.Error(xerrors.New(resp.ErrCode)),
			zfield.Eid(en.ID), zfield.ReqID(reqID), zfield.Base(en.JSON()))
		return nil, xerrors.New(resp.ErrCode)
	}

	// decode response.
	var apiResp dao.Entity
	if err = dao.GetEntityCodec().Decode(resp.Data, &apiResp); nil != err {
		log.Error("patch entity configs, decode response",
			zfield.ReqID(reqID), zap.Error(err), zfield.Eid(en.ID), zfield.Base(en.JSON()))
		return nil, errors.Wrap(err, "patch entity configs, decode response")
	}

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return &Base{
		ID:         apiResp.ID,
		Type:       apiResp.Type,
		Owner:      apiResp.Owner,
		Source:     apiResp.Source,
		Properties: apiResp.Properties,
	}, nil
}

// QueryConfigs query entity configs.
func (m *apiManager) GetEntityConfigs(ctx context.Context, en *Base, propertyIDs []string) (*Base, error) {
	var (
		err    error
		entity *dao.Entity
	)

	reqID := util.UUID()
	elapsedTime := util.NewElapsed()
	log.Info("entity.QueryConfigs", zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.ReqID(reqID), zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// get entity config file.
	if entity, err = m.entityRepo.GetEntity(ctx, &dao.Entity{ID: en.ID}); nil != err {
		log.Error("query entity configs", zap.Error(err), zfield.Eid(en.ID), zfield.ReqID(reqID))
		return nil, errors.Wrap(err, "query entity configs")
	}

	// get properties by ids.
	cc := collectjs.ByteNew(entity.ConfigFile)
	configs := make(map[string]*constraint.Config)
	for _, propertyID := range propertyIDs {
		var cfg *constraint.Config
		bytes := cc.Get(propertyID).GetRaw()
		if cfg, err = constraint.ParseFrom(bytes); nil != err {
			log.Error("parse entity configs", zap.Error(err), zfield.Eid(en.ID))
			return nil, errors.Wrap(err, "parse entity configs")
		}
		configs[propertyID] = cfg
	}

	base := entityToBase(entity)
	base.Configs = configs

	log.Info("processing completed", zfield.Eid(en.ID),
		zfield.ReqID(reqID), zfield.Elapsed(elapsedTime.Elapsed()))

	return base, nil
}

// AppendMapper append a mapper into entity.
func (m *apiManager) AppendMapper(ctx context.Context, en *Base) error {
	log.Info("entity.AppendMapper",
		zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// upert mapper.
	var err error
	mp := en.Mappers[0]
	if err = m.entityRepo.PutMapper(ctx, &dao.Mapper{
		ID:          mp.ID,
		TQL:         mp.TQL,
		Name:        mp.Name,
		EntityID:    en.ID,
		EntityType:  en.Type,
		Description: mp.Description,
	}); nil != err {
		log.Error("append mapper", zap.Error(err), zfield.Eid(en.ID))
		return errors.Wrap(err, "append mapper")
	}

	return nil
}

// DeleteMapper delete mapper from entity.
func (m *apiManager) RemoveMapper(ctx context.Context, en *Base) error {
	log.Info("entity.RemoveMapper",
		zfield.Eid(en.ID), zfield.Type(en.Type),
		zfield.Owner(en.Owner), zfield.Source(en.Source), zfield.Base(en.JSON()))

	// delete mapper.
	var err error
	mp := en.Mappers[0]
	if err = m.entityRepo.DelMapper(ctx, &dao.Mapper{
		ID:          mp.ID,
		TQL:         mp.TQL,
		Name:        mp.Name,
		EntityID:    en.ID,
		EntityType:  en.Type,
		Description: mp.Description,
	}); nil != err {
		log.Error("remove mapper", zap.Error(err), zfield.Eid(en.ID))
		return errors.Wrap(err, "remove mapper")
	}

	return nil
}

func (m *apiManager) CheckSubscription(ctx context.Context, en *Base) (err error) {
	// check TQLs.
	if err = checkTQLs(en); nil != err {
		return errors.Wrap(err, "check subscription")
	}

	// check request.
	mode := getString(en.Properties[subscription.SubscriptionFieldMode])
	topic := getString(en.Properties[subscription.SubscriptionFieldTopic])
	filter := getString(en.Properties[subscription.SubscriptionFieldFilter])
	pubsubName := getString(en.Properties[subscription.SubscriptionFieldPubsubName])
	log.Infof("check subscription, mode: %s, topic: %s, filter:%s, pubsub: %s, source: %s", mode, topic, filter, pubsubName, en.Source)
	if mode == subscription.SubscriptionModeUndefine || en.Source == "" || filter == "" || topic == "" || pubsubName == "" {
		log.Error("create subscription", zap.Error(runtime.ErrSubscriptionInvalid), zap.String("subscription", en.ID))
		return runtime.ErrSubscriptionInvalid
	}

	return nil
}

var eventType = message.MessageTypeAPIRequest.String()

func (m *apiManager) makeEvent(en *dao.Entity) (cloudevents.Event, error) {
	var err error
	var bytes []byte
	ev := cloudevents.NewEvent()

	ev.SetID(util.UUID())
	ev.SetType(eventType)
	ev.SetSource(en.Source)
	ev.SetExtension(message.ExtEntityID, en.ID)
	ev.SetExtension(message.ExtEntityType, en.Type)
	ev.SetExtension(message.ExtEntityOwner, en.Owner)
	ev.SetExtension(message.ExtMessageReceiver, en.ID)
	ev.SetExtension(message.ExtEntitySource, en.Source)
	ev.SetExtension(message.ExtCallback, m.callbackAddr())
	ev.SetExtension(message.ExtMessageSender, eventSender)
	ev.SetDataContentType(cloudevents.ApplicationJSON)

	// encode request & set event payload.
	if bytes, err = dao.GetEntityCodec().Encode(en); nil != err {
		log.Error("encode request", zap.Error(err), zfield.Eid(en.ID))
		return ev, errors.Wrap(err, "encode api.request")
	} else if err = ev.SetData(bytes); nil != err {
		log.Error("set event payload", zap.Error(err), zfield.Eid(en.ID))
		return ev, errors.Wrap(err, "set event payload")
	} else if err = ev.Validate(); nil != err {
		log.Error("validate event", zap.Error(err), zfield.Eid(en.ID))
		return ev, errors.Wrap(err, "validate event")
	}

	return ev, nil
}

func (m *apiManager) makePatchEvent(en *dao.Entity, pds []state.PatchData) (cloudevents.Event, error) {
	var err error
	var bytes []byte
	ev := cloudevents.NewEvent()

	ev.SetID(util.UUID())
	ev.SetType(eventType)
	ev.SetSource(en.Source)
	ev.SetExtension(message.ExtEntityID, en.ID)
	ev.SetExtension(message.ExtEntityType, en.Type)
	ev.SetExtension(message.ExtEntityOwner, en.Owner)
	ev.SetExtension(message.ExtMessageReceiver, en.ID)
	ev.SetExtension(message.ExtEntitySource, en.Source)
	ev.SetExtension(message.ExtCallback, m.callbackAddr())
	ev.SetExtension(message.ExtMessageSender, eventSender)
	ev.SetDataContentType(cloudevents.ApplicationJSON)

	// encode request & set event payload.
	if bytes, err = state.GetPatchCodec().Encode(pds); nil != err {
		log.Error("encode request", zap.Error(err), zfield.Eid(en.ID))
		return ev, errors.Wrap(err, "encode api.request")
	} else if err = ev.SetData(bytes); nil != err {
		log.Error("set event payload", zap.Error(err), zfield.Eid(en.ID))
		return ev, errors.Wrap(err, "set event payload")
	} else if err = ev.Validate(); nil != err {
		log.Error("validate event", zap.Error(err), zfield.Eid(en.ID))
		return ev, errors.Wrap(err, "validate event")
	}

	return ev, nil
}

func checkTQLs(en *Base) error {
	// check TQL.
	var err error
	defer func() {
		defer func() {
			switch recover() {
			case nil:
			default:
				err = ErrMapperTQLInvalid
			}
		}()
	}()
	for _, mm := range en.Mappers {
		var tqlInst tql.TQL
		if tqlInst, err = tql.NewTQL(mm.TQL); nil != err {
			log.Error("append mapper", zap.Error(err), zfield.Eid(en.ID))
			return errors.Wrap(err, "check TQL")
		} else if tqlInst.Target() != en.ID {
			log.Error("mismatched subscription id & mapper target id.", zfield.Eid(en.ID), zap.Any("mapper", mm))
			return errors.Wrap(err, "subscription ID mismatched")
		}
	}
	return errors.Wrap(err, "check TQL")
}

func getString(node constraint.Node) string {
	if nil != node {
		return node.String()
	}
	return ""
}