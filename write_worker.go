package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	qnats "github.com/rqure/qlib/pkg/data/store/nats"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WriteWorker interface {
	app.Worker
	OnStoreConnected(context.Context)
	OnStoreDisconnected()
}

type writeWorker struct {
	store            data.Store
	natsCore         qnats.Core
	isStoreConnected bool
	modeManager      ModeManager
	handle           app.Handle
}

func NewWriteWorker(store data.Store) WriteWorker {
	return &writeWorker{
		store: store,
	}
}

func (w *writeWorker) Deinit(context.Context) {}
func (w *writeWorker) DoWork(context.Context) {}
func (w *writeWorker) OnStoreConnected(ctx context.Context) {
	w.isStoreConnected = true
}

func (w *writeWorker) OnStoreDisconnected() {
	w.isStoreConnected = false
}

func (w *writeWorker) Init(ctx context.Context, handle app.Handle) {
	if !w.modeManager.HasModes(ModeWrite) {
		return
	}

	w.handle = handle
	w.natsCore.QueueSubscribe(w.natsCore.GetKeyGenerator().GetWriteSubject(), w.handleWriteRequest)
}

func (w *writeWorker) handleWriteRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg protobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			log.Error("Could not unmarshal message: %v", err)
			return
		}

		switch {
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigCreateEntityRequest{}):
			w.handleCreateEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigDeleteEntityRequest{}):
			w.handleDeleteEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigSetEntitySchemaRequest{}):
			w.handleSetEntitySchema(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiConfigRestoreSnapshotRequest{}):
			w.handleRestoreSnapshot(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&protobufs.ApiRuntimeDatabaseRequest{}):
			w.handleDatabaseRequest(ctx, msg, &apiMsg)
		}
	})
}

func (w *writeWorker) handleCreateEntity(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigCreateEntityRequest)
	rsp := new(protobufs.ApiConfigCreateEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		rsp.Status = protobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	id := w.store.CreateEntity(ctx, req.Type, req.ParentId, req.Name)
	rsp.Id = id
	rsp.Status = protobufs.ApiConfigCreateEntityResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleDeleteEntity(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigDeleteEntityRequest)
	rsp := new(protobufs.ApiConfigDeleteEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		rsp.Status = protobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	w.store.DeleteEntity(ctx, req.Id)
	rsp.Status = protobufs.ApiConfigDeleteEntityResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleSetEntitySchema(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigSetEntitySchemaRequest)
	rsp := new(protobufs.ApiConfigSetEntitySchemaResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		rsp.Status = protobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	w.store.SetEntitySchema(ctx, entity.FromSchemaPb(req.Schema))
	rsp.Status = protobufs.ApiConfigSetEntitySchemaResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleRestoreSnapshot(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiConfigRestoreSnapshotRequest)
	rsp := new(protobufs.ApiConfigRestoreSnapshotResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		rsp.Status = protobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = protobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	w.store.RestoreSnapshot(ctx, snapshot.FromPb(req.Snapshot))
	rsp.Status = protobufs.ApiConfigRestoreSnapshotResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleDatabaseRequest(ctx context.Context, msg *nats.Msg, apiMsg *protobufs.ApiMessage) {
	req := new(protobufs.ApiRuntimeDatabaseRequest)
	rsp := new(protobufs.ApiRuntimeDatabaseResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		log.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isStoreConnected {
		log.Error("Could not handle request %v. Database is not connected.", req)
		w.sendResponse(msg, rsp)
		return
	}

	if req.RequestType != protobufs.ApiRuntimeDatabaseRequest_WRITE {
		log.Error("Only WRITE requests are supported")
		w.sendResponse(msg, rsp)
		return
	}

	reqs := []data.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, request.FromPb(r))
	}

	log.Info("Write request: %v", req.Requests)
	w.store.Write(ctx, reqs...)

	rsp.Response = req.Requests
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) sendResponse(msg *nats.Msg, response proto.Message) {
	if msg.Reply == "" {
		return
	}

	apiMsg := &protobufs.ApiMessage{
		Header: &protobufs.ApiHeader{
			AuthenticationStatus: protobufs.ApiHeader_AUTHENTICATED,
			Timestamp:            timestamppb.Now(),
		},
	}

	if err := apiMsg.Payload.MarshalFrom(response); err != nil {
		log.Error("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		log.Error("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		log.Error("Could not send response: %v", err)
	}
}
