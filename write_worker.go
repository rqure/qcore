package main

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WriteWorker interface {
	qapp.Worker
	OnReady(context.Context)
	OnNotReady(context.Context)
}

type writeWorker struct {
	store       *qdata.Store
	natsCore    qnats.NatsCore
	isReady     bool
	modeManager ModeManager

	handle qcontext.Handle
}

func NewWriteWorker(store *qdata.Store, natsCore qnats.NatsCore, modeManager ModeManager) WriteWorker {
	return &writeWorker{
		store:       store,
		natsCore:    natsCore,
		modeManager: modeManager,
	}
}

func (w *writeWorker) Deinit(context.Context) {}
func (w *writeWorker) DoWork(context.Context) {}
func (w *writeWorker) OnReady(ctx context.Context) {
	w.isReady = true

	if w.modeManager.HasModes(ModeWrite) {
		w.natsCore.QueueSubscribe(
			w.natsCore.GetKeyGenerator().GetWriteSubject(),
			qcontext.GetAppName(ctx),
			w.handleWriteRequest)
	}
}

func (w *writeWorker) OnNotReady(context.Context) {
	w.isReady = false
}

func (w *writeWorker) Init(ctx context.Context) {
	w.handle = qcontext.GetHandle(ctx)
}

func (w *writeWorker) handleWriteRequest(msg *nats.Msg) {
	w.handle.DoInMainThread(func(ctx context.Context) {
		var apiMsg qprotobufs.ApiMessage
		if err := proto.Unmarshal(msg.Data, &apiMsg); err != nil {
			qlog.Error("Could not unmarshal message: %v", err)
			return
		}

		switch {
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigCreateEntityRequest{}):
			w.handleCreateEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigDeleteEntityRequest{}):
			w.handleDeleteEntity(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigSetEntitySchemaRequest{}):
			w.handleSetEntitySchema(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiConfigRestoreSnapshotRequest{}):
			w.handleRestoreSnapshot(ctx, msg, &apiMsg)
		case apiMsg.Payload.MessageIs(&qprotobufs.ApiRuntimeDatabaseRequest{}):
			w.handleDatabaseRequest(ctx, msg, &apiMsg)
		}
	})
}

func (w *writeWorker) handleCreateEntity(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigCreateEntityRequest)
	rsp := new(qprotobufs.ApiConfigCreateEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	id := w.store.CreateEntity(
		ctx,
		qdata.EntityType(req.Type),
		qdata.EntityId(req.ParentId),
		req.Name)

	rsp.Id = id.AsString()
	rsp.Status = qprotobufs.ApiConfigCreateEntityResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleDeleteEntity(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigDeleteEntityRequest)
	rsp := new(qprotobufs.ApiConfigDeleteEntityResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	w.store.DeleteEntity(ctx, qdata.EntityId(req.Id))
	rsp.Status = qprotobufs.ApiConfigDeleteEntityResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleSetEntitySchema(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigSetEntitySchemaRequest)
	rsp := new(qprotobufs.ApiConfigSetEntitySchemaResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	w.store.SetEntitySchema(ctx, new(qdata.EntitySchema).FromEntitySchemaPb(req.Schema))
	rsp.Status = qprotobufs.ApiConfigSetEntitySchemaResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleRestoreSnapshot(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiConfigRestoreSnapshotRequest)
	rsp := new(qprotobufs.ApiConfigRestoreSnapshotResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_FAILURE
		w.sendResponse(msg, rsp)
		return
	}

	w.store.RestoreSnapshot(ctx, new(qdata.Snapshot).FromSnapshotPb(req.Snapshot))
	rsp.Status = qprotobufs.ApiConfigRestoreSnapshotResponse_SUCCESS
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) handleDatabaseRequest(ctx context.Context, msg *nats.Msg, apiMsg *qprotobufs.ApiMessage) {
	req := new(qprotobufs.ApiRuntimeDatabaseRequest)
	rsp := new(qprotobufs.ApiRuntimeDatabaseResponse)

	if err := apiMsg.Payload.UnmarshalTo(req); err != nil {
		qlog.Error("Could not unmarshal request: %v", err)
		w.sendResponse(msg, rsp)
		return
	}

	if !w.isReady {
		qlog.Error("Could not handle request %v. Database is not connected.", req)
		w.sendResponse(msg, rsp)
		return
	}

	if req.RequestType != qprotobufs.ApiRuntimeDatabaseRequest_WRITE {
		qlog.Error("Only WRITE requests are supported")
		w.sendResponse(msg, rsp)
		return
	}

	reqs := []*qdata.Request{}
	for _, r := range req.Requests {
		reqs = append(reqs, new(qdata.Request).FromRequestPb(r))
	}

	qlog.Info("Write request: %v", req.Requests)
	clientProvider := qcontext.GetClientProvider[qauth.Client](ctx)
	client := clientProvider.Client(ctx)
	if client != nil {
		accessorSession := client.AccessTokenToSession(ctx, apiMsg.Header.AccessToken)

		if !accessorSession.IsValid(ctx) {
			qlog.Warn("Invalid session")
			return
		}

		accessorName, err := accessorSession.GetOwnerName(ctx)
		if err != nil {
			qlog.Error("Could not get owner name: %v", err)
			return
		}

		found := false
		iterator := w.store.PrepareQuery("SELECT Name FROM User WHERE Name = %q", accessorName)
		for iterator.Next(ctx) {
			user := iterator.Get()

			w.store.Write(
				context.WithValue(
					ctx,
					qcontext.KeyAuthorizer,
					qauthorization.NewAuthorizer(user.EntityId, w.store)),
				reqs...)

			found = true

			// Break after first user
			break
		}

		if !found {
			iterator := w.store.PrepareQuery("SELECT Name FROM Client WHERE Name = %q", accessorName)

			for iterator.Next(ctx) {
				client := iterator.Get()
				w.store.Write(
					context.WithValue(
						ctx,
						qcontext.KeyAuthorizer,
						qauthorization.NewAuthorizer(client.EntityId, w.store)),
					reqs...)

				// Break after first client
				break
			}
		}
	}

	rsp.Response = req.Requests
	w.sendResponse(msg, rsp)
}

func (w *writeWorker) sendResponse(msg *nats.Msg, response proto.Message) {
	if msg.Reply == "" {
		return
	}

	apiMsg := &qprotobufs.ApiMessage{
		Header: &qprotobufs.ApiHeader{
			Timestamp: timestamppb.Now(),
		},
	}

	var err error
	apiMsg.Payload, err = anypb.New(response)
	if err != nil {
		qlog.Error("Could not marshal response: %v", err)
		return
	}

	data, err := proto.Marshal(apiMsg)
	if err != nil {
		qlog.Error("Could not marshal message: %v", err)
		return
	}

	if err := msg.Respond(data); err != nil {
		qlog.Error("Could not send response: %v", err)
	}
}
